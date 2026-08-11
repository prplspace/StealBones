"""
OpenSea — раздел 3.1 ТЗ. API v2, события коллекции (sales).

Работает и без ключа (сильно урезанный лимит), с ключом — по X-API-KEY.
Получение ключа — см. приложение "Ключи API" (два варианта: мгновенный
временный ключ на 30 дней без регистрации, либо постоянный через форму
в личном кабинете).

ВАЖНО: точная форма полей seller/buyer в ответе v2 (плоская строка или
вложенный объект с "address") не перепроверена вживую (нет сетевого доступа
в среде разработки) — код обрабатывает оба варианта защитно.

ДОБАВЛЕНО (08.08.2026, жалоба пользователя "0 кошельков по OpenSea всегда"):
раньше этот адаптер был single-shot — один запрос, максимум 50 событий, и
всё, независимо от target_wallets (см. историю в fetch_activity ниже). По
официальному changelog OpenSea (docs.opensea.io/changelog/cursor-pagination,
раздел "Cursor Pagination") /events поддерживает курсорную пагинацию с 2022
года — и именно она сняла прежнее ограничение offset-пагинации в 10 000
записей, так что технической причины останавливаться после одной страницы
нет. Теперь есть fetch_activity_page (используется pipeline.py так же, как у
Magic Eden — см. supports_deep_search ниже), которая листает по курсору,
пока не наберётся нужное число кошельков С НУЖНЫМ БАЛАНСОМ, до конца
доступной истории или потолка страниц (MAX_PAGES_SAFETY_CEILING в
pipeline.py — здесь нет своего MAX_OFFSET вроде magic_eden.py, потому что
курсорная пагинация по определению не имеет фиксированного числового
потолка вроде HTTP 400 на конкретном offset).
ЧЕСТНО: имя поля курсора в реальном ответе (в доке — "next"; у некоторых
сторонних обёрток OpenSea встречается "next_cursor") не перепроверено
вживую — код проверяет оба варианта защитно, как и с buyer/seller.
fetch_activity (единственный запрос) оставлен как есть — для обратной
совместимости и старых тестов, симметрично magic_eden.py.

СЕДЬМОЙ РАУНД (08.08.2026):
1. HTTP 429 распознаётся отдельно от обрыва соединения/конца курсора
   (RateLimited, не AdapterError — см. rate_limit/guard.py, запрос
   пользователя п.4) — курсор при этом НЕ сбрасывается, pipeline.py делает
   паузу и продолжает с того же места.
2. fetch_holders_page() — второй, независимый источник кандидатов: текущие
   держатели коллекции (эндпоинт /collection/{slug}/nfts), а не лента
   недавних сделок. Не ограничен глубиной истории активности — ограничен
   только размером коллекции. По запросу пользователя (п.1: "не теряем ли
   кошельки из-за глубины истории") — pipeline.py включает эту фазу ПОСЛЕ
   исчерпания обычной ленты активности, если цель по кошелькам ещё не
   достигнута. ЧЕСТНО: путь и форма ответа — по официальному opensea-js
   (метод getNFTsByCollection) и стороннему описанию, НЕ проверены вживую.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import requests

from adapters.marketplaces.base import ActivityRecord, AdapterError, MarketplaceAdapter
from config import settings
from rate_limit.guard import RateLimited, parse_retry_after, with_backoff

logger = logging.getLogger("steal_bones.adapters.opensea")

BASE_URL = "https://api.opensea.io/api/v2"


class OpenSeaAdapter(MarketplaceAdapter):
    name = "opensea"
    requires_key = False  # опционален, но сильно влияет на лимит
    default_daily_limit = 86_400  # эвристика для ключа с постоянным доступом; без ключа — намного ниже
    SUPPORTED_ASSET_TYPES = {"nft"}
    supports_deep_search = True  # см. docstring модуля — курсорная пагинация добавлена 08.08.2026
    supports_holder_scan = True  # ДОБАВЛЕНО (седьмой раунд) — см. fetch_holders_page ниже

    def __init__(self) -> None:
        pass

    def fetch_activity(self, asset_type: str, target: str, limit: int = 100, target_wallets: int = 20, network: str = "ethereum") -> list[ActivityRecord]:
        if asset_type != "nft":
            raise AdapterError("OpenSea — NFT-площадка, для мемкоинов используйте dexscreener/birdeye")

        url = f"{BASE_URL}/events/collection/{target}"
        headers = {"User-Agent": settings.user_agent, "Accept": "application/json"}
        if settings.opensea_key:
            headers["X-API-KEY"] = settings.opensea_key

        params = {"event_type": "sale", "limit": min(limit, 50)}
        logger.info("OpenSea: запрос %s params=%s", url, params)
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=15)
            logger.info("OpenSea: HTTP %s, длина тела %s байт", resp.status_code, len(resp.content))
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            raise AdapterError(f"OpenSea: сбой запроса событий для {target}: {exc}") from exc

        return self._parse_events(data.get("asset_events", []), target, network)

    def fetch_activity_page(self, asset_type: str, target: str, offset: int = 0, limit: int = 50,
                             network: str = "ethereum", cursor: str | None = None) -> tuple[list[ActivityRecord], str | None, bool]:
        """Постраничный примитив для pipeline.py (см. docstring модуля)."""
        if asset_type != "nft":
            raise AdapterError("OpenSea — NFT-площадка, для мемкоинов используйте dexscreener/birdeye")

        url = f"{BASE_URL}/events/collection/{target}"
        headers = {"User-Agent": settings.user_agent, "Accept": "application/json"}
        if settings.opensea_key:
            headers["X-API-KEY"] = settings.opensea_key

        params = {"event_type": "sale", "limit": 50}
        if cursor:
            params["next"] = cursor

        logger.info("OpenSea: запрос %s params=%s", url, params)

        def _do_request():
            resp = requests.get(url, params=params, headers=headers, timeout=15)
            if resp.status_code == 429:
                raise RateLimited(
                    f"OpenSea: 429 на {url} (курсор={cursor})",
                    retry_after=parse_retry_after(resp.headers.get("Retry-After")),
                    source="opensea",
                )
            return resp

        try:
            resp = with_backoff(_do_request, retries=2, base_delay=0.8, retry_on=(requests.ConnectionError, requests.Timeout))
            logger.info("OpenSea: HTTP %s, длина тела %s байт", resp.status_code, len(resp.content))
            resp.raise_for_status()
            data = resp.json()
        except RateLimited:
            raise  # пусть pipeline.py решает паузу — курсор НЕ сброшен, продолжим с него же
        except requests.RequestException as exc:
            if offset == 0 and not cursor:
                raise AdapterError(f"OpenSea: сбой запроса событий для {target}: {exc}") from exc
            logger.warning("OpenSea: страница (курсор=%s) не удалась даже после повторов (%s) — останавливаемся", cursor, exc)
            return [], None, False

        events = data.get("asset_events", [])
        records = self._parse_events(events, target, network)

        next_cursor = data.get("next")
        next_cursor_str = str(next_cursor) if next_cursor is not None else None
        return records, next_cursor_str, bool(next_cursor_str)

    def fetch_holders_page(self, target: str, offset: int = 0, limit: int = 50,
                            network: str = "ethereum", cursor: str | None = None) -> tuple[list[ActivityRecord], str | None, bool]:
        """
        ДОБАВЛЕНО (седьмой раунд, 08.08.2026) — по запросу пользователя,
        проверка альтернативы ленте активности.
        """
        url = f"{BASE_URL}/collection/{target}/nfts"
        headers = {"User-Agent": settings.user_agent, "Accept": "application/json"}
        if settings.opensea_key:
            headers["X-API-KEY"] = settings.opensea_key

        params = {"limit": 50}
        if cursor:
            params["next"] = cursor

        logger.info("OpenSea (держатели коллекции): запрос %s params=%s", url, params)

        def _do_request():
            resp = requests.get(url, params=params, headers=headers, timeout=15)
            if resp.status_code == 429:
                raise RateLimited(
                    f"OpenSea (держатели): 429 на {url} (курсор={cursor})",
                    retry_after=parse_retry_after(resp.headers.get("Retry-After")),
                    source="opensea",
                )
            return resp

        try:
            resp = with_backoff(_do_request, retries=2, base_delay=0.8, retry_on=(requests.ConnectionError, requests.Timeout))
            logger.info("OpenSea (держатели): HTTP %s, длина тела %s байт", resp.status_code, len(resp.content))
            resp.raise_for_status()
            data = resp.json()
        except RateLimited:
            raise
        except requests.RequestException as exc:
            if offset == 0 and not cursor:
                logger.warning("OpenSea (держатели): эндпоинт недоступен или не в ожидаемой форме для %s (%s) — пропускаем эту фазу", target, exc)
                return [], None, False
            logger.warning("OpenSea (держатели): страница (курсор=%s) не удалась (%s) — останавливаемся", cursor, exc)
            return [], None, False

        records = []
        for nft in data.get("nfts", []):
            for owner_entry in nft.get("owners", []):
                addr = owner_entry.get("address")
                if addr:
                    records.append(ActivityRecord(
                        wallet_address=addr,
                        role="holder",
                        network=network,
                        asset_id=target,
                        price=None,
                        timestamp=None,
                    ))

        next_cursor = data.get("next")
        next_cursor_str = str(next_cursor) if next_cursor is not None else None
        return records, next_cursor_str, bool(next_cursor_str)

    @staticmethod
    def _parse_events(events: list[dict], target: str, network: str) -> list[ActivityRecord]:
        def _addr(value):
            if isinstance(value, dict):
                return value.get("address")
            return value

        records: list[ActivityRecord] = []
        for ev in events:
            buyer = _addr(ev.get("buyer"))
            seller = _addr(ev.get("seller"))
            closing = ev.get("closing_date")
            ts = datetime.fromtimestamp(closing, tz=timezone.utc) if isinstance(closing, (int, float)) else None
            payment = ev.get("payment") or {}
            price = None
            if payment.get("quantity") is not None and payment.get("decimals") is not None:
                try:
                    price = int(payment["quantity"]) / (10 ** int(payment["decimals"]))
                except (ValueError, TypeError):
                    price = None

            for addr, role in ((buyer, "buyer"), (seller, "seller")):
                if addr:
                    records.append(ActivityRecord(
                        wallet_address=addr, role=role, network=network,
                        asset_id=target, price=price, timestamp=ts,
                    ))
        return records

    def check_collection_exists(self, slug: str) -> dict | None:
        url = f"{BASE_URL}/collections/{slug}"
        headers = {"User-Agent": settings.user_agent, "Accept": "application/json"}
        if settings.opensea_key:
            headers["X-API-KEY"] = settings.opensea_key
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            raise AdapterError(f"OpenSea: сбой проверки коллекции {slug}: {exc}") from exc
        return {"symbol": data.get("collection", slug), "name": data.get("name", slug)}
