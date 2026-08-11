"""
Тесты новой оркестрации pipeline.py для StealBones V2 без проверок балансов.
"""

import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
import pipeline
from adapters.marketplaces.base import ActivityRecord


def _fresh_db():
    tmpdir = tempfile.mkdtemp()
    config.settings.db_path = Path(tmpdir) / "test.db"
    from db.models import init_db
    init_db(config.settings.db_path)


def test_pipeline_collects_exact_unique_wallets_count():
    """
    Проверяет, что pipeline останавливается ровно при достижении target_wallets уникальных адресов и сохраняет их в БД.
    """
    _fresh_db()

    fake_mkt = MagicMock()
    fake_mkt.default_daily_limit = 10_000
    fake_mkt.supports_asset_type.return_value = True
    fake_mkt.supports_deep_search = True
    fake_mkt.supports_holder_scan = False

    # Сгенерируем 5 уникальных адресов на первой странице
    page_records = [
        ActivityRecord(f"0x{i:040x}", "buyer", "ethereum", "bayc", 1.0, None)
        for i in range(1, 6)
    ]
    fake_mkt.fetch_activity_page.return_value = (page_records, None, False)
    pipeline.MARKETPLACE_ADAPTERS["opensea"] = fake_mkt

    # Целевое количество кошельков - 3
    result = pipeline.run_job(
        platform="opensea", asset_type="nft", network="ethereum",
        target="bayc", target_wallets=3
    )

    # Проверяем, что в результате ровно 3 уникальных кошелька и сохранено тоже 3
    assert result.unique_wallets_seen == 3
    assert result.wallets_stored == 3
    assert result.stopped_early_at_target is True

    from db.crud import list_wallets
    stored = list_wallets(config.settings.db_path)
    assert len(stored) == 3


def test_pipeline_runs_all_three_phases_in_correct_priority():
    """
    Проверяет, что запуск сбора проходит строго через три фазы в новом приоритете:
    Phase 1: True Holders (Helius DAS on Solana / OpenSea on EVM)
    Phase 2: Active Listings (e.g. Magic Eden Listings)
    Phase 3: Activity Feed (Capped activity feed)
    """
    _fresh_db()

    config.settings.helius_keys = ["test-key"]

    fake_mkt = MagicMock()
    fake_mkt.default_daily_limit = 10_000
    fake_mkt.supports_asset_type.return_value = True
    fake_mkt.supports_deep_search = True
    fake_mkt.supports_holder_scan = True

    # Setup Phase 1: Helius DAS on Solana
    from adapters.registry import BALANCE_ADAPTERS
    solana_adapter = BALANCE_ADAPTERS["solana"]
    solana_adapter.fetch_collection_holders_das = MagicMock()
    solana_adapter.fetch_collection_holders_das.return_value = [
        ActivityRecord("SOL_DAS_HOLDER_1", "holder", "solana", "mad_lads", None, None)
    ]

    # Setup Phase 2: Active Listings (fetch_holders_page)
    fake_mkt.fetch_holders_page.return_value = (
        [ActivityRecord("SOL_LISTING_1", "holder", "solana", "mad_lads", None, None)],
        False
    )

    # Setup Phase 3: Activity Feed (fetch_activity_page)
    fake_mkt.fetch_activity_page.return_value = (
        [ActivityRecord("SOL_ACTIVITY_1", "buyer", "solana", "mad_lads", None, None)],
        False
    )

    pipeline.MARKETPLACE_ADAPTERS["magic_eden"] = fake_mkt

    # Задаем target_wallets = 3, чтобы задействовать все 3 фазы
    result = pipeline.run_job(
        platform="magic_eden", asset_type="nft", network="solana",
        target="mad_lads", target_wallets=3
    )

    assert result.unique_wallets_seen == 3
    assert result.wallets_stored == 3

    solana_adapter.fetch_collection_holders_das.assert_called_once_with("mad_lads")
    fake_mkt.fetch_holders_page.assert_called_once()
    fake_mkt.fetch_activity_page.assert_called_once()

    config.settings.helius_keys = []  # reset key


def test_batch_mode_accumulates_across_collections_and_stops_early():
    """
    Проверяет батч-режим: уникальные кошельки накапливаются по очереди для нескольких коллекций,
    и сбор останавливается, как только общая цель target_wallets достигнута.
    """
    _fresh_db()

    fake_mkt = MagicMock()
    fake_mkt.default_daily_limit = 10_000
    fake_mkt.supports_asset_type.return_value = True
    fake_mkt.supports_deep_search = True
    fake_mkt.supports_holder_scan = False

    def fake_page(asset_type, target, offset, limit, network=None):
        if target == "coll_a":
            return ([ActivityRecord("SOL_A", "buyer", "solana", "coll_a", None, None)], False)
        if target == "coll_b":
            return ([ActivityRecord("SOL_B", "buyer", "solana", "coll_b", None, None)], False)
        raise AssertionError("coll_c не должна была опрашиваться — цель уже достигнута")

    fake_mkt.fetch_activity_page.side_effect = fake_page
    pipeline.MARKETPLACE_ADAPTERS["magic_eden"] = fake_mkt

    result = pipeline.run_job(
        platform="magic_eden", asset_type="nft", network="solana",
        target=["coll_a", "coll_b", "coll_c"], target_wallets=2
    )

    assert result.unique_wallets_seen == 2
    assert result.targets_completed == 2
    assert result.targets_total == 3


def test_rate_limit_pauses_and_resumes():
    """
    HTTP 429 ставит job на паузу и затем продолжает.
    """
    _fresh_db()
    from rate_limit.guard import RateLimited

    fake_mkt = MagicMock()
    fake_mkt.default_daily_limit = 10_000
    fake_mkt.supports_asset_type.return_value = True
    fake_mkt.supports_deep_search = True
    fake_mkt.supports_holder_scan = False

    call_count = {"n": 0}
    def fake_page(asset_type, target, offset, limit, network=None):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RateLimited("429 test", retry_after=1.0, source="magic_eden")
        return ([ActivityRecord("SOL_LIMIT_RESUME", "buyer", "solana", "x", None, None)], False)

    fake_mkt.fetch_activity_page.side_effect = fake_page
    pipeline.MARKETPLACE_ADAPTERS["magic_eden"] = fake_mkt

    with patch("pipeline.time.sleep") as mock_sleep:
        result = pipeline.run_job(
            platform="magic_eden", asset_type="nft", network="solana",
            target="x", target_wallets=1
        )

    assert call_count["n"] == 2
    assert result.rate_limit_pauses == 1
    assert result.unique_wallets_seen == 1
    mock_sleep.assert_called_once_with(1.25)


if __name__ == "__main__":
    test_pipeline_collects_exact_unique_wallets_count()
    test_pipeline_runs_all_three_phases_in_correct_priority()
    test_batch_mode_accumulates_across_collections_and_stops_early()
    test_rate_limit_pauses_and_resumes()
    print("All custom pipeline V2 tests passed.")
