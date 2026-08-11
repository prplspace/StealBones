"""
Тесты адаптеров на замоканных HTTP-ответах.

ВАЖНО: это НЕ проверка того, что реальные Magic Eden/Solscan/Etherscan и
т.д. действительно отвечают в таком формате (в среде, где писался этот
код, не было сетевого доступа для проверки вживую). Это проверка, что КОД
ПАРСИНГА корректно обрабатывает ответ ИМЕННО ТАКОЙ формы, какую я предположил
по документации. Если реальный API отдаёт данные в другой форме — тесты
не спасут, нужно свериться с живым ответом и поправить _parse_* методы.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from adapters.balances.bitcoin import BitcoinBalanceAdapter
from adapters.balances.evm import EvmBalanceAdapter
from adapters.balances.solana import SolanaBalanceAdapter
from adapters.balances.sui import SuiBalanceAdapter
from adapters.balances.tron import TronBalanceAdapter
from adapters.marketplaces.magic_eden import MagicEdenAdapter
from adapters.marketplaces.opensea import OpenSeaAdapter


def _mock_response(json_data, status_ok=True):
    resp = MagicMock()
    resp.json.return_value = json_data
    resp.status_code = 200 if status_ok else 400
    resp.raise_for_status.side_effect = None if status_ok else Exception("HTTP error")
    return resp


def test_solana_balance():
    fake_response = _mock_response({"jsonrpc": "2.0", "result": {"context": {"slot": 1}, "value": 2_500_000_000}, "id": 1})
    with patch("adapters.balances.solana.requests.post", return_value=fake_response) as mock_post:
        adapter = SolanaBalanceAdapter()
        balance = adapter.get_balance("SomeSolanaAddress111")
        assert balance == 2.5, f"Ожидалось 2.5 SOL, получено {balance}"
        assert mock_post.call_args.kwargs["json"]["method"] == "getBalance"
    print("OK: SolanaBalanceAdapter корректно переводит lamports -> SOL")


def test_solana_multiple_helius_keys_rotate_not_concatenate():
    """Регресс-тест на реальный баг (лог пользователя 05.08.2026): пользователь
    ввёл 3 ключа Helius через запятую (как и предлагает подсказка в Settings),
    а старый код склеивал их в один "ключ" в URL -> Helius отвечал 401 на
    каждый запрос. Теперь это должна быть настоящая ротация: каждый ключ
    подставляется в URL ПО ОТДЕЛЬНОСТИ, не через запятую."""
    import config
    import tempfile
    from pathlib import Path
    tmpdir = tempfile.mkdtemp()
    config.settings.db_path = Path(tmpdir) / "test.db"
    from db.models import init_db
    init_db(config.settings.db_path)

    config.settings.helius_keys = ["key-AAA", "key-BBB", "key-CCC"]

    fake_response = _mock_response({"jsonrpc": "2.0", "result": {"value": 1_000_000_000}, "id": 1})
    urls_used = []

    def fake_post(url, **kwargs):
        urls_used.append(url)
        return fake_response

    with patch("adapters.balances.solana.requests.post", side_effect=fake_post):
        adapter = SolanaBalanceAdapter()
        adapter.get_balance("ADDR1")

    assert len(urls_used) == 1
    url = urls_used[0]
    assert "," not in url, f"БАГ ВЕРНУЛСЯ: ключи склеены запятой в один URL: {url}"
    assert url.endswith("key-AAA") or "key-AAA" in url
    print("OK: несколько ключей Helius через запятую используются по одному за раз (ротация), а не склеиваются в один")

    config.settings.helius_keys = []  # не оставляем состояние для других тестов


def test_evm_balance():
    # 1.5 ETH в wei = 1500000000000000000 = 0x14d1120d7b160000
    fake_response = _mock_response({"jsonrpc": "2.0", "id": 1, "result": hex(1_500_000_000_000_000_000)})
    with patch("adapters.balances.evm.requests.post", return_value=fake_response):
        adapter = EvmBalanceAdapter("ethereum")
        balance = adapter.get_balance("0xabc")
        assert balance == 1.5, f"Ожидалось 1.5 ETH, получено {balance}"
    print("OK: EvmBalanceAdapter корректно переводит wei (hex) -> ETH")


def test_evm_balance_falls_back_to_next_rpc_on_connection_error():
    """Регресс-тест на реальный баг из лога пользователя 08.08.2026: ~60
    подряд SSLError на eth.llamarpc.com молча "съедали" каждый адрес (см.
    docstring evm.py). Первый узел в списке пусть падает сразу и на всех
    повторах, второй — отвечает нормально; итог должен быть успешным
    балансом со второго узла, а не BalanceCheckError."""
    import requests as requests_module
    ok_response = _mock_response({"jsonrpc": "2.0", "id": 1, "result": hex(2_000_000_000_000_000_000)})

    call_log = []
    def fake_post(url, **kwargs):
        call_log.append(url)
        if "llamarpc" in url:
            raise requests_module.exceptions.SSLError("EOF occurred in violation of protocol")
        return ok_response

    with patch("adapters.balances.evm.requests.post", side_effect=fake_post), \
         patch("rate_limit.guard.time.sleep", lambda *_: None):  # не ждать реальные секунды между попытками в тесте
        adapter = EvmBalanceAdapter("ethereum")
        balance = adapter.get_balance("0xabc")

    assert balance == 2.0, f"Ожидалось 2.0 ETH со второго узла, получено {balance}"
    assert any("llamarpc" in u for u in call_log), "Первый (проблемный) узел должен был быть опробован"
    assert any("llamarpc" not in u for u in call_log), "Должен был произойти переход на резервный узел"
    print("OK: EvmBalanceAdapter переключается на резервный RPC, если основной недоступен (не глотает адрес молча)")


def test_evm_balance_raises_with_clear_message_when_all_rpcs_fail():
    """Если ВСЕ узлы (включая резервные) недоступны — по-прежнему должно
    бросаться BalanceCheckError (адрес не должен тихо получать баланс 0),
    но с сообщением, из которого видно, что дело в RPC, а не в самом адресе."""
    import requests as requests_module
    from adapters.balances.base import BalanceCheckError

    def always_fail(url, **kwargs):
        raise requests_module.exceptions.ConnectionError("connection refused")

    with patch("adapters.balances.evm.requests.post", side_effect=always_fail), \
         patch("rate_limit.guard.time.sleep", lambda *_: None):
        adapter = EvmBalanceAdapter("ethereum")
        try:
            adapter.get_balance("0xabc")
            assert False, "Ожидался BalanceCheckError"
        except BalanceCheckError as exc:
            assert "недоступны" in str(exc)
    print("OK: EvmBalanceAdapter корректно бросает BalanceCheckError, когда все узлы недоступны (не притворяется балансом 0)")


def test_bitcoin_balance():
    fake_response = _mock_response({
        "chain_stats": {"funded_txo_sum": 300_000_000, "spent_txo_sum": 100_000_000},
        "mempool_stats": {"funded_txo_sum": 0, "spent_txo_sum": 0},
    })
    with patch("adapters.balances.bitcoin.requests.get", return_value=fake_response):
        adapter = BitcoinBalanceAdapter()
        balance = adapter.get_balance("bc1someaddress")
        assert balance == 2.0, f"Ожидалось 2.0 BTC, получено {balance}"
    print("OK: BitcoinBalanceAdapter корректно считает funded - spent")


def test_tron_balance_activated():
    fake_response = _mock_response({"address": "41abc", "balance": 5_000_000})
    with patch("adapters.balances.tron.requests.post", return_value=fake_response):
        adapter = TronBalanceAdapter()
        balance = adapter.get_balance("TSomeAddress")
        assert balance == 5.0, f"Ожидалось 5.0 TRX, получено {balance}"
    print("OK: TronBalanceAdapter корректно переводит sun -> TRX (активированный аккаунт)")


def test_tron_balance_unactivated():
    fake_response = _mock_response({})  # неактивированный адрес — пустой объект
    with patch("adapters.balances.tron.requests.post", return_value=fake_response):
        adapter = TronBalanceAdapter()
        balance = adapter.get_balance("TNeverUsedAddress")
        assert balance == 0.0
    print("OK: TronBalanceAdapter не падает на неактивированном адресе (баланс 0)")


def test_sui_balance():
    fake_response = _mock_response({
        "jsonrpc": "2.0", "id": 1,
        "result": {"coinType": "0x2::sui::SUI", "coinObjectCount": 3, "totalBalance": "3000000000", "lockedBalance": {}},
    })
    with patch("adapters.balances.sui.requests.post", return_value=fake_response):
        adapter = SuiBalanceAdapter()
        balance = adapter.get_balance("0xsuiaddress")
        assert balance == 3.0, f"Ожидалось 3.0 SUI, получено {balance}"
    print("OK: SuiBalanceAdapter корректно переводит MIST -> SUI")


def test_magic_eden_activity_parsing():
    fake_activities = [
        {"type": "buyNow", "buyer": "BuyerWallet1", "seller": "SellerWallet1", "price": 12.5, "blockTime": 1700000000},
        {"type": "bid", "buyer": "BidderWallet2", "price": None, "blockTime": 1700000100},  # заявка без seller
        {"type": "list", "seller": "SellerWallet3", "price": None, "blockTime": 1700000200},  # листинг без buyer
        {"type": "delist", "price": None, "blockTime": 1700000300},  # ни buyer, ни seller — реально пусто, не считаем
    ]
    fake_response = _mock_response(fake_activities)
    with patch("adapters.marketplaces.magic_eden.requests.get", return_value=fake_response):
        adapter = MagicEdenAdapter()
        records = adapter.fetch_activity("nft", "mad_lads")
        addrs_roles = {(r.wallet_address, r.role) for r in records}
        # Продажа — buyer И seller оба размечены соответствующими ролями (как раньше)
        assert ("BuyerWallet1", "buyer") in addrs_roles
        assert ("SellerWallet1", "seller") in addrs_roles
        assert ("SellerWallet3", "lister") in addrs_roles
        # Учитываем одиночные bid события как bidder
        assert ("BidderWallet2", "bidder") in addrs_roles
        assert all(r.network == "solana" for r in records)
        assert len(records) == 4  # 2 (продажа) + 1 (bidder) + 1 (lister); полностью пустая запись — не считается
    print("OK: MagicEdenAdapter учитывает продажи (buyer+seller), листинги/делистинги (lister) и одиночные ставки (bidder)")


def test_opensea_activity_parsing_nested_addresses():
    fake_data = {
        "asset_events": [
            {
                "buyer": {"address": "0xBuyer"}, "seller": {"address": "0xSeller"},
                "closing_date": 1700000000,
                "payment": {"quantity": "1000000000000000000", "decimals": 18, "symbol": "ETH"},
            }
        ]
    }
    fake_response = _mock_response(fake_data)
    with patch("adapters.marketplaces.opensea.requests.get", return_value=fake_response):
        adapter = OpenSeaAdapter()
        records = adapter.fetch_activity("nft", "boredapeyachtclub")
        assert len(records) == 2
        buyer_rec = next(r for r in records if r.role == "buyer")
        assert buyer_rec.wallet_address == "0xBuyer"
        assert buyer_rec.price == 1.0  # 1e18 wei / 10^18 = 1.0 ETH
    print("OK: OpenSeaAdapter корректно достаёт адрес из вложенного объекта {'address': ...}")


def test_magic_eden_pagination_goes_deeper_when_no_sales_on_first_page():
    """Страница 1 — 500 записей БЕЗ buyer и БЕЗ seller (например, служебные
    записи вроде отмены аукциона без сторон) -> ни одной ActivityRecord с
    неё, но страница полная -> код должен пойти на страницу 2. Страница 2
    короче лимита (последняя) и содержит реальную продажу."""
    page1 = [{"type": "auction_cancelled", "price": None, "blockTime": 1} for i in range(500)]
    page2 = [
        {"type": "buyNow", "buyer": "REAL_BUYER", "seller": "REAL_SELLER", "price": 5.0, "blockTime": 2},
        {"type": "bid", "buyer": "BIDDER_X", "price": 1.0, "blockTime": 3},
    ]

    call_count = {"n": 0}

    def fake_get(*args, **kwargs):
        call_count["n"] += 1
        return _mock_response(page1 if call_count["n"] == 1 else page2)

    with patch("adapters.marketplaces.magic_eden.requests.get", side_effect=fake_get):
        adapter = MagicEdenAdapter()
        records = adapter.fetch_activity("nft", "test_collection", limit=500)

    addrs = {(r.wallet_address, r.role) for r in records}
    assert call_count["n"] == 2, f"Ожидалось 2 запроса, было {call_count['n']}"
    assert ("REAL_BUYER", "buyer") in addrs
    assert ("REAL_SELLER", "seller") in addrs
    # BIDDER_X (ставка без продавца) теперь учитывается
    assert ("BIDDER_X", "bidder") in addrs
    print("OK: MagicEdenAdapter листает на следующую страницу, если текущая не даёт ни одного кандидата, и находит их дальше")


def test_opensea_pagination_follows_cursor_across_pages():
    """Регресс/новая функциональность (08.08.2026): раньше OpenSeaAdapter был
    single-shot (максимум 50 событий, независимо от target_wallets). Теперь
    fetch_activity_page должен листать по курсору (next), пока он есть."""
    page1 = {
        "asset_events": [
            {"buyer": "B1", "seller": "S1", "payment": {"quantity": "1000000000000000000", "decimals": 18}},
        ],
        "next": "CURSOR_ABC",
    }
    page2 = {
        "asset_events": [
            {"buyer": "B2", "seller": "S2", "payment": {"quantity": "2000000000000000000", "decimals": 18}},
        ],
        "next": None,
    }

    calls = []
    def fake_get(url, params=None, **kwargs):
        calls.append(dict(params or {}))
        return _mock_response(page1 if len(calls) == 1 else page2)

    with patch("adapters.marketplaces.opensea.requests.get", side_effect=fake_get):
        adapter = OpenSeaAdapter()
        records1, next_cursor1, has_more1 = adapter.fetch_activity_page("nft", "test-collection", 0, 50, network="ethereum")
        assert has_more1 is True, "После первой страницы должен быть курсор 'next' — есть смысл листать дальше"
        assert "next" not in calls[0], "На первой странице курсора ещё быть не должно"

        records2, next_cursor2, has_more2 = adapter.fetch_activity_page("nft", "test-collection", 50, 50, network="ethereum", cursor=next_cursor1)
        assert has_more2 is False, "На второй странице next=None — дальше листать некуда"
        assert calls[1].get("next") == "CURSOR_ABC", "Вторая страница должна была запросить именно тот курсор, что вернула первая"

    addrs = {r.wallet_address for r in records1 + records2}
    assert addrs == {"B1", "S1", "B2", "S2"}
    print("OK: OpenSeaAdapter.fetch_activity_page корректно листает по курсору next между вызовами")


def test_opensea_pagination_resets_cursor_for_new_collection():
    """Если offset==0 (новая коллекция — в т.ч. следующая в батче, см.
    pipeline.py) — старый курсор от предыдущей коллекции использоваться не должен."""
    resp_a = {"asset_events": [{"buyer": "A1", "seller": "A2"}], "next": "CURSOR_A"}
    resp_b = {"asset_events": [{"buyer": "B1", "seller": "B2"}], "next": None}

    calls = []
    def fake_get(url, params=None, **kwargs):
        calls.append(dict(params or {}))
        return _mock_response(resp_a if len(calls) == 1 else resp_b)

    with patch("adapters.marketplaces.opensea.requests.get", side_effect=fake_get):
        adapter = OpenSeaAdapter()
        adapter.fetch_activity_page("nft", "collection-a", 0, 50, network="ethereum")
        # Новая коллекция, offset снова 0 — курсор от collection-a не должен утечь сюда
        adapter.fetch_activity_page("nft", "collection-b", 0, 50, network="ethereum")

    assert "next" not in calls[1], "Курсор предыдущей коллекции не должен использоваться для новой"
    print("OK: OpenSeaAdapter сбрасывает курсор при переходе на новую коллекцию (offset=0)")


def test_magic_eden_fetch_holders_page():
    fake_listings = [
        {"seller": "SellerX", "price": 10.0},
        {"sellerAddress": "SellerY", "price": None},
        {"something_else": "NoSeller"}
    ]
    fake_response = _mock_response(fake_listings)
    with patch("adapters.marketplaces.magic_eden.requests.get", return_value=fake_response) as mock_get:
        adapter = MagicEdenAdapter()
        records, has_more = adapter.fetch_holders_page("mad_lads", 0, 100)

        assert len(records) == 2
        assert records[0].wallet_address == "SellerX"
        assert records[0].role == "holder"
        assert records[0].price == 10.0
        assert records[1].wallet_address == "SellerY"
        assert records[1].role == "holder"
        assert records[1].price is None
        assert has_more is False
        assert "listings" in mock_get.call_args[0][0]
    print("OK: MagicEdenAdapter.fetch_holders_page parses listings successfully")


def test_solana_balance_with_native_stake_and_lst():
    # 1. Mock SOL price (CoinGecko)
    coingecko_resp = _mock_response({"solana": {"usd": 100.0}})

    # 2. Mock native balance (getBalance)
    native_resp = _mock_response({"jsonrpc": "2.0", "result": {"value": 1_000_000_000}, "id": 1}) # 1.0 SOL

    # 3. Mock native stake (getParsedProgramAccounts)
    stake_resp_staker = _mock_response({
        "jsonrpc": "2.0",
        "result": [
            {"pubkey": "STAKE_KEY_1", "account": {"lamports": 2_000_000_000}}
        ],
        "id": 1
    })
    stake_resp_withdrawer = _mock_response({
        "jsonrpc": "2.0",
        "result": [
            {"pubkey": "STAKE_KEY_1", "account": {"lamports": 2_000_000_000}},
            {"pubkey": "STAKE_KEY_2", "account": {"lamports": 1_000_000_000}}
        ],
        "id": 1
    })

    # 4. Mock SPL token holdings
    token_resp = _mock_response({
        "jsonrpc": "2.0",
        "result": {
            "value": [
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "mint": "J1t124M1sB369xR2yX6zA4E6A4vC7D4m27S4n8b8pump",
                                    "tokenAmount": {"uiAmount": 5.0}
                                }
                            }
                        }
                    }
                },
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "mint": "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
                                    "tokenAmount": {"uiAmount": 2.0}
                                }
                            }
                        }
                    }
                },
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "mint": "5oVNBe1RmvBovB4nhgDjA7twC5hJw22vS9A3B31",
                                    "tokenAmount": {"uiAmount": 1.0}
                                }
                            }
                        }
                    }
                }
            ]
        },
        "id": 1
    })

    escrow_resp = _mock_response({"balance": 500_000_000}) # 0.5 SOL

    def fake_get(url, *args, **kwargs):
        if "coingecko" in url:
            return coingecko_resp
        if "escrow_balance" in url:
            return escrow_resp
        return _mock_response({})

    gppa_calls = []

    def fake_post(url, json=None, **kwargs):
        method = json.get("method")
        if method == "getBalance":
            return native_resp
        if method == "getParsedProgramAccounts":
            filters = json.get("params", [])[1].get("filters", [])
            offset = filters[0].get("memcmp", {}).get("offset")
            gppa_calls.append(offset)
            if offset == 12:
                return stake_resp_staker
            elif offset == 44:
                return stake_resp_withdrawer
        if method == "getTokenAccountsByOwner":
            return token_resp
        return _mock_response({})

    import time
    with patch("adapters.balances.solana.requests.get", side_effect=fake_get), \
         patch("adapters.balances.solana.requests.post", side_effect=fake_post):
        from adapters.balances import solana
        solana._sol_price_cache = {"price": 100.0, "timestamp": time.time()}
        solana._token_prices_cache = {
            "JitoSOL": 110.0,
            "mSOL": 120.0,
            "bSOL": 115.0,
            "INF": 130.0,
            "timestamp": time.time()
        }

        adapter = SolanaBalanceAdapter()
        balance = adapter.get_balance("SomeSolanaAddressWithExtras")

        assert balance == 13.7, f"Expected 13.7 SOL, got {balance}"
        assert len(gppa_calls) == 2
        assert set(gppa_calls) == {12, 44}
    print("OK: SolanaBalanceAdapter correctly sums native balance, native stake and LST balances")


def test_solana_batch_balance_with_escrow_and_stablecoins():
    # Mock responses for:
    # 1. SOL/USD Price (CoinGecko)
    # 2. Native SOL (getMultipleAccounts JSON-RPC)
    # 3. Magic Eden escrow balance (REST)
    # 4. SPL stablecoins (getTokenAccountsByOwner JSON-RPC)

    # Let's mock CoinGecko SOL price
    coingecko_resp = _mock_response({"solana": {"usd": 100.0}})

    # Let's mock Native SOL for ADDR1 and ADDR2 via getMultipleAccounts
    # ADDR1 has 1,500,000,000 lamports (1.5 SOL)
    # ADDR2 has 2,000,000,000 lamports (2.0 SOL)
    native_resp = _mock_response({
        "jsonrpc": "2.0",
        "result": {
            "context": {"slot": 1},
            "value": [
                {"lamports": 1_500_000_000},
                {"lamports": 2_000_000_000}
            ]
        },
        "id": 1
    })

    # Let's mock Magic Eden Escrow balance
    # ADDR1 has 500,000,000 lamports (0.5 SOL) in escrow
    # ADDR2 has 1,000,000,000 lamports (1.0 SOL) in escrow
    escrow_resp1 = _mock_response({"balance": 500_000_000})
    escrow_resp2 = _mock_response({"balance": 1_000_000_000})

    # Let's mock SPL token holdings
    # ADDR1 has 100.0 USDC (mint: EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v)
    token_resp1 = _mock_response({
        "jsonrpc": "2.0",
        "result": {
            "value": [
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                                    "tokenAmount": {"uiAmount": 100.0}
                                }
                            }
                        }
                    }
                }
            ]
        },
        "id": 1
    })
    # ADDR2 has 50.0 USDT (mint: Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB)
    token_resp2 = _mock_response({
        "jsonrpc": "2.0",
        "result": {
            "value": [
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                                    "tokenAmount": {"uiAmount": 50.0}
                                }
                            }
                        }
                    }
                }
            ]
        },
        "id": 1
    })

    # We mock requests.get and requests.post
    def fake_get(url, *args, **kwargs):
        if "coingecko" in url:
            return coingecko_resp
        if "escrow_balance" in url:
            if "ADDR1" in url:
                return escrow_resp1
            if "ADDR2" in url:
                return escrow_resp2
        return _mock_response({})

    def fake_post(url, json=None, **kwargs):
        method = json.get("method")
        if method == "getMultipleAccounts":
            return native_resp
        if method == "getTokenAccountsByOwner":
            params = json.get("params") or []
            if "ADDR1" in params:
                return token_resp1
            if "ADDR2" in params:
                return token_resp2
        return _mock_response({})

    with patch("adapters.balances.solana.requests.get", side_effect=fake_get), \
         patch("adapters.balances.solana.requests.post", side_effect=fake_post):
        # Reset solana price cache to force fetch
        from adapters.balances import solana
        solana._sol_price_cache = {"price": 150.0, "timestamp": 0.0}

        adapter = SolanaBalanceAdapter()
        balances = adapter.get_balances(["ADDR1", "ADDR2"])

        # ADDR1 expected: 1.5 (native) + 0.5 (escrow) + 100 USDC / 100 (SOL Price) = 3.0 SOL
        # ADDR2 expected: 2.0 (native) + 1.0 (escrow) + 50 USDT / 100 (SOL Price) = 3.5 SOL
        assert balances["ADDR1"] == 3.0, f"Expected 3.0 for ADDR1, got {balances['ADDR1']}"
        assert balances["ADDR2"] == 3.5, f"Expected 3.5 for ADDR2, got {balances['ADDR2']}"
    print("OK: SolanaBalanceAdapter пакетный расчет баланса с эскроу и стейблкоинами работает отлично")


def test_fetch_mmm_pools():
    """
    Тестирует fetch_mmm_pools() в MagicEdenAdapter на различных ответах:
    1. Успешный список пулов.
    2. Пустой список.
    3. Некорректные/не-списочные ответы и отсутствие 'owner'.
    4. HTTP 400/404 ошибки.
    """
    adapter = MagicEdenAdapter()

    # 1. Успешный список пулов
    standard_resp = [
        {"pool": "pool_a", "owner": "OwnerWallet1"},
        {"pool": "pool_b", "owner": "OwnerWallet2"},
        {"pool": "pool_c", "something_else": "no_owner"},
        "invalid_element"
    ]
    fake_response = _mock_response(standard_resp)
    with patch("adapters.marketplaces.magic_eden.requests.get", return_value=fake_response) as mock_get:
        records = adapter.fetch_mmm_pools("mad_lads")
        assert len(records) == 2
        assert records[0].wallet_address == "OwnerWallet1"
        assert records[0].role == "liquidity_provider"
        assert records[0].network == "solana"
        assert records[0].price is None
        assert records[0].timestamp is None
        assert records[1].wallet_address == "OwnerWallet2"
        mock_get.assert_called_once()

    # 2. Пустой список
    fake_response = _mock_response([])
    with patch("adapters.marketplaces.magic_eden.requests.get", return_value=fake_response):
        records = adapter.fetch_mmm_pools("mad_lads")
        assert records == []

    # 3. Некорректные/не-списочные ответы
    dict_resp = {"error": "some api error"}
    fake_response = _mock_response(dict_resp)
    with patch("adapters.marketplaces.magic_eden.requests.get", return_value=fake_response):
        records = adapter.fetch_mmm_pools("mad_lads")
        assert records == []

    # 4. HTTP 400/404 ошибки
    fake_response = _mock_response({}, status_ok=False)
    with patch("adapters.marketplaces.magic_eden.requests.get", return_value=fake_response):
        records = adapter.fetch_mmm_pools("mad_lads")
        assert records == []

    print("OK: MagicEdenAdapter.fetch_mmm_pools() протестирован успешно на всех типах ответов")


def test_solana_helius_das_holders():
    """
    Проверяет метод fetch_collection_holders_das на SolanaBalanceAdapter:
    - Правильный POST запрос
    - Валидация items[].ownership.owner
    - Грациозный возврат собранных данных при ошибке
    """
    import config
    import tempfile
    from pathlib import Path
    tmpdir = tempfile.mkdtemp()
    config.settings.db_path = Path(tmpdir) / "test.db"
    from db.models import init_db
    init_db(config.settings.db_path)

    config.settings.helius_keys = ["test-helius-key"]

    # 1. Successful paginated scenario
    # Page 1: returns 1000 items (means has_more)
    page1_items = [{"ownership": {"owner": f"Owner_{i}"}} for i in range(1000)]
    page1_resp = _mock_response({
        "jsonrpc": "2.0",
        "result": {
            "items": page1_items
        },
        "id": "get-holders"
    })

    # Page 2: returns 500 items (less than 1000, means stop pagination)
    page2_items = [{"ownership": {"owner": f"Owner_{i+1000}"}} for i in range(500)]
    page2_resp = _mock_response({
        "jsonrpc": "2.0",
        "result": {
            "items": page2_items
        },
        "id": "get-holders"
    })

    responses = [page1_resp, page2_resp]
    post_calls = []

    def fake_post(url, json=None, **kwargs):
        post_calls.append((url, json))
        return responses[len(post_calls) - 1]

    with patch("adapters.balances.solana.requests.post", side_effect=fake_post):
        adapter = SolanaBalanceAdapter()
        records = adapter.fetch_collection_holders_das("some_mint_abc")

    assert len(records) == 1500
    assert records[0].wallet_address == "Owner_0"
    assert records[1499].wallet_address == "Owner_1499"
    assert records[0].role == "holder"
    assert records[0].network == "solana"
    assert records[0].asset_id == "some_mint_abc"

    assert len(post_calls) == 2
    assert "api-key=test-helius-key" in post_calls[0][0]
    assert post_calls[0][1]["params"]["page"] == 1
    assert post_calls[1][1]["params"]["page"] == 2

    # 2. Connection error on page 2 - should gracefully return first page records (1000)
    import requests as requests_module
    post_calls_err = []

    def fake_post_err(url, json=None, **kwargs):
        post_calls_err.append((url, json))
        if len(post_calls_err) == 1:
            return page1_resp
        raise requests_module.exceptions.ConnectionError("timeout")

    with patch("adapters.balances.solana.requests.post", side_effect=fake_post_err):
        adapter = SolanaBalanceAdapter()
        records_err = adapter.fetch_collection_holders_das("some_mint_abc")

    # Should gracefully return the 1000 items retrieved from Page 1
    assert len(records_err) == 1000
    assert records_err[-1].wallet_address == "Owner_999"

    config.settings.helius_keys = []  # reset


if __name__ == "__main__":
    test_solana_balance()
    test_solana_multiple_helius_keys_rotate_not_concatenate()
    test_evm_balance()
    test_evm_balance_falls_back_to_next_rpc_on_connection_error()
    test_evm_balance_raises_with_clear_message_when_all_rpcs_fail()
    test_bitcoin_balance()
    test_tron_balance_activated()
    test_tron_balance_unactivated()
    test_sui_balance()
    test_magic_eden_activity_parsing()
    test_magic_eden_pagination_goes_deeper_when_no_sales_on_first_page()
    test_opensea_activity_parsing_nested_addresses()
    test_opensea_pagination_follows_cursor_across_pages()
    test_opensea_pagination_resets_cursor_for_new_collection()
    test_magic_eden_fetch_holders_page()
    test_solana_balance_with_native_stake_and_lst()
    test_solana_batch_balance_with_escrow_and_stablecoins()
    test_fetch_mmm_pools()
    test_solana_helius_das_holders()
    print()
    print("=" * 60)
    print("ВСЕ ТЕСТЫ ПРОШЛИ — но см. предупреждение в начале файла про")
    print("реальную форму ответов внешних API.")
    print("=" * 60)
