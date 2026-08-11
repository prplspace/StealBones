"""
Magic Eden — раздел 3.1 / 3.3 ТЗ.
Переработано для ультра-быстрого сбора уникальных кошельков без проверки балансов.
"""

from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timezone

import requests

from adapters.marketplaces.base import ActivityRecord, AdapterError, MarketplaceAdapter
from config import settings
from rate_limit.guard import RateLimited, magic_eden_limiter, parse_retry_after, with_backoff

logger = logging.getLogger("steal_bones.adapters.magic_eden")

BASE_URL = "https://api-mainnet.magiceden.dev/v2"
PAGE_SIZE = 500
MAX_OFFSET = 15000


def _is_valid_pubkey(s: str) -> bool:
    if not isinstance(s, str):
        return False
    if not (32 <= len(s) <= 44):
        return False
    allowed = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
    return all(c in allowed for c in s)


class MagicEdenAdapter(MarketplaceAdapter):
    name = "magic_eden"
    requires_key = False
    default_daily_limit = 172_800
    SUPPORTED_ASSET_TYPES = {"nft"}
    supports_deep_search = True
    supports_holder_scan = True

    def get_collection_mint(self, symbol: str) -> str | None:
        """
        Выполняет GET-запрос к https://api-mainnet.magiceden.dev/v2/collections/{symbol}.
        Если запрос успешен и возвращает JSON-объект, проверяет наличие полей primaryContract, firstVerifiedCreator.
        Если адрес не найден, выполняет резервный запрос к https://api-mainnet.magiceden.dev/v2/collections/{symbol}/listings?limit=1.
        Извлекает из первого элемента массива поле mint или tokenAddress.
        Возвращает строку, если это валидный Base58 Pubkey, иначе None.
        """
        url = f"{BASE_URL}/collections/{symbol}"
        logger.info("Magic Eden: get_collection_mint для %s", symbol)

        def _do_primary_request():
            magic_eden_limiter.wait_and_consume()
            return requests.get(url, headers={"User-Agent": settings.user_agent}, timeout=15)

        try:
            resp = with_backoff(_do_primary_request, retries=2, base_delay=0.8, retry_on=(requests.ConnectionError, requests.Timeout))
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict):
                    # 1. primaryContract
                    p_contract = data.get("primaryContract")
                    if p_contract and _is_valid_pubkey(p_contract):
                        return p_contract

                    # 2. firstVerifiedCreator
                    creator = data.get("firstVerifiedCreator")
                    if creator:
                        if isinstance(creator, str) and _is_valid_pubkey(creator):
                            return creator
                        elif isinstance(creator, list) and creator:
                            first_creator = creator[0]
                            if isinstance(first_creator, str) and _is_valid_pubkey(first_creator):
                                return first_creator
        except Exception as exc:
            logger.warning("Magic Eden: ошибка при первичном запросе get_collection_mint для %s: %s", symbol, exc)

        # Fallback request
        fallback_url = f"{BASE_URL}/collections/{symbol}/listings"
        params = {"limit": 1}
        logger.info("Magic Eden: резервный запрос get_collection_mint для %s", symbol)

        def _do_fallback_request():
            magic_eden_limiter.wait_and_consume()
            return requests.get(fallback_url, params=params, headers={"User-Agent": settings.user_agent}, timeout=15)

        try:
            resp = with_backoff(_do_fallback_request, retries=2, base_delay=0.8, retry_on=(requests.ConnectionError, requests.Timeout))
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and data:
                    first_item = data[0]
                    if isinstance(first_item, dict):
                        mint = first_item.get("mint")
                        if mint and _is_valid_pubkey(mint):
                            return mint
                        token_addr = first_item.get("tokenAddress")
                        if token_addr and _is_valid_pubkey(token_addr):
                            return token_addr
        except Exception as exc:
            logger.warning("Magic Eden: ошибка при резервном запросе get_collection_mint для %s: %s", symbol, exc)

        return None

    def fetch_wallets_rank(self, target: str) -> list[ActivityRecord]:
        """
        Immediately returns [] to suppress HTTP 400 logs.
        """
        return []

    def fetch_mmm_pools(self, target: str) -> list[ActivityRecord]:
        """
        Выполняет GET-запрос к эндпоинту AMM-пулов коллекции:
        GET https://api-mainnet.magiceden.dev/v2/mmm/pools?collectionSymbol={target}
        Парсит полученные пулы с ролью "liquidity_provider".
        """
        url = f"{BASE_URL}/mmm/pools"
        params = {"collectionSymbol": target}
        logger.info("Magic Eden MMM Pools: запрос %s params=%s", url, params)

        def _do_request():
            magic_eden_limiter.wait_and_consume()
            base_delay = 1.0
            for attempt in range(3):
                resp = requests.get(url, params=params, headers={"User-Agent": settings.user_agent}, timeout=15)
                if resp.status_code == 429:
                    jitter = random.uniform(0.5, 1.5)
                    delay = base_delay * (2 ** attempt) + jitter
                    logger.warning("Magic Eden MMM Pools: HTTP 429. Retry attempt %s after %.2fs...", attempt + 1, delay)
                    time.sleep(delay)
                    if attempt == 2:
                        raise RateLimited(
                            f"Magic Eden MMM Pools: 429 на {url}",
                            retry_after=parse_retry_after(resp.headers.get("Retry-After")),
                            source="magic_eden",
                        )
                    continue

                if resp.status_code in (400, 404):
                    return resp

                return resp
            return resp

        try:
            resp = with_backoff(_do_request, retries=2, base_delay=0.8, retry_on=(requests.ConnectionError, requests.Timeout))
            logger.info("Magic Eden MMM Pools: HTTP %s", resp.status_code)
            if resp.status_code in (400, 404):
                return []
            resp.raise_for_status()
            data = resp.json()
        except RateLimited:
            raise
        except requests.RequestException as exc:
            logger.warning("Magic Eden MMM Pools: сбой запроса для %s (%s)", target, exc)
            return []

        # Defensive parsing
        items = data if isinstance(data, list) else []
        records: list[ActivityRecord] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            owner = item.get("owner")
            if owner and isinstance(owner, str):
                records.append(ActivityRecord(
                    wallet_address=owner,
                    role="liquidity_provider",
                    network="solana",
                    asset_id=target,
                    price=None,
                    timestamp=None,
                ))
        return records

    def fetch_holders_page(self, target: str, offset: int, limit: int = 100, network: str = "solana") -> tuple[list[ActivityRecord], bool]:
        if offset > MAX_OFFSET:
            return [], False

        url = f"{BASE_URL}/collections/{target}/listings"
        params = {"offset": offset, "limit": limit}
        logger.info("Magic Eden Listings: запрос %s params=%s", url, params)

        def _do_request():
            magic_eden_limiter.wait_and_consume()
            base_delay = 1.0
            for attempt in range(3):
                resp = requests.get(url, params=params, headers={"User-Agent": settings.user_agent}, timeout=15)
                if resp.status_code == 429:
                    jitter = random.uniform(0.5, 1.5)
                    delay = base_delay * (2 ** attempt) + jitter
                    logger.warning("Magic Eden Listings: HTTP 429. Retry attempt %s after %.2fs...", attempt + 1, delay)
                    time.sleep(delay)
                    if attempt == 2:
                        raise RateLimited(
                            f"Magic Eden Listings: 429 на {url} (offset={offset})",
                            retry_after=parse_retry_after(resp.headers.get("Retry-After")),
                            source="magic_eden",
                        )
                    continue

                if resp.status_code in (400, 404):
                    return resp

                return resp
            return resp

        try:
            resp = with_backoff(_do_request, retries=2, base_delay=0.8, retry_on=(requests.ConnectionError, requests.Timeout))
            logger.info("Magic Eden Listings: HTTP %s, длина тела %s байт", resp.status_code, len(resp.content))
            if resp.status_code in (400, 404):
                return [], False
            resp.raise_for_status()
            data = resp.json()
        except RateLimited:
            raise
        except requests.RequestException as exc:
            if offset == 0:
                raise AdapterError(f"Magic Eden Listings: сбой запроса для {target}: {exc}") from exc
            logger.warning("Magic Eden Listings: страница на offset=%s не удалась даже после повторов (%s)", offset, exc)
            return [], False

        items = data if isinstance(data, list) else []
        if not items:
            return [], False

        records: list[ActivityRecord] = []
        for item in items:
            seller = item.get("seller") or item.get("sellerAddress")
            if seller:
                records.append(ActivityRecord(
                    wallet_address=seller,
                    role="holder",
                    network="solana",
                    asset_id=target,
                    price=float(item.get("price")) if item.get("price") is not None else None,
                    timestamp=None,
                ))

        has_more = len(items) == limit and (offset + limit) < MAX_OFFSET
        return records, has_more

    def fetch_activity_page(self, asset_type: str, target: str, offset: int, limit: int = PAGE_SIZE,
                             network: str = "solana") -> tuple[list[ActivityRecord], bool]:
        if asset_type != "nft":
            raise AdapterError("Magic Eden — NFT-площадка, для мемкоинов используйте dexscreener/birdeye")
        if offset >= MAX_OFFSET:
            logger.warning("Magic Eden history depth limit (15000) reached")
            return [], False

        url = f"{BASE_URL}/collections/{target}/activities"
        params = {"offset": offset, "limit": limit}
        logger.info("Magic Eden: запрос %s params=%s", url, params)

        def _do_request():
            magic_eden_limiter.wait_and_consume()
            base_delay = 1.0
            for attempt in range(3):
                resp = requests.get(url, params=params, headers={"User-Agent": settings.user_agent}, timeout=15)
                if resp.status_code == 429:
                    jitter = random.uniform(0.5, 1.5)
                    delay = base_delay * (2 ** attempt) + jitter
                    logger.warning("Magic Eden: HTTP 429. Retry attempt %s after %.2fs...", attempt + 1, delay)
                    time.sleep(delay)
                    if attempt == 2:
                        raise RateLimited(
                            f"Magic Eden: 429 на {url} (offset={offset})",
                            retry_after=parse_retry_after(resp.headers.get("Retry-After")),
                            source="magic_eden",
                        )
                    continue

                if resp.status_code in (400, 404):
                    return resp

                return resp
            return resp

        try:
            resp = with_backoff(_do_request, retries=2, base_delay=0.8, retry_on=(requests.ConnectionError, requests.Timeout))
            logger.info("Magic Eden: HTTP %s, длина тела %s байт", resp.status_code, len(resp.content))
            if resp.status_code in (400, 404):
                return [], False
            resp.raise_for_status()
            data = resp.json()
        except RateLimited:
            raise
        except requests.RequestException as exc:
            if offset == 0:
                raise AdapterError(f"Magic Eden: сбой запроса активности для {target}: {exc}") from exc
            logger.warning("Magic Eden: страница на offset=%s не удалась даже после повторов (%s)", offset, exc)
            return [], False

        items = data if isinstance(data, list) else (data.get("activities", []) if isinstance(data, dict) else [])
        if not items:
            return [], False

        records = self._parse_items(items, target)
        has_more = len(items) == limit and (offset + limit) < MAX_OFFSET
        return records, has_more

    def fetch_activity(self, asset_type: str, target: str, limit: int = 100, target_wallets: int = 20) -> list[ActivityRecord]:
        all_records: list[ActivityRecord] = []
        unique: set[str] = set()
        offset = 0
        while True:
            page_records, has_more = self.fetch_activity_page(asset_type, target, offset, PAGE_SIZE)
            all_records.extend(page_records)
            unique.update(r.wallet_address for r in page_records)
            if len(unique) >= target_wallets or not has_more:
                break
            offset += PAGE_SIZE
        return all_records

    @staticmethod
    def _parse_items(items: list[dict], target: str) -> list[ActivityRecord]:
        records: list[ActivityRecord] = []
        for item in items:
            buyer = item.get("buyer") or item.get("buyerAddress")
            seller = item.get("seller") or item.get("sellerAddress")
            if not buyer and not seller:
                continue

            block_time = item.get("blockTime")
            ts = datetime.fromtimestamp(block_time, tz=timezone.utc) if isinstance(block_time, (int, float)) else None
            price = item.get("price")

            if buyer and seller:
                pairs = ((buyer, "buyer"), (seller, "seller"))
            elif buyer:
                pairs = ((buyer, "bidder"),)
            elif seller:
                pairs = ((seller, "lister"),)
            else:
                continue

            for addr, role in pairs:
                if addr:
                    records.append(ActivityRecord(
                        wallet_address=addr, role=role, network="solana",
                        asset_id=target, price=float(price) if price is not None else None,
                        timestamp=ts,
                    ))
        return records

    def check_collection_exists(self, symbol: str) -> dict | None:
        magic_eden_limiter.wait_and_consume()
        url = f"{BASE_URL}/collections/{symbol}/stats"
        try:
            resp = requests.get(url, headers={"User-Agent": settings.user_agent}, timeout=10)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            raise AdapterError(f"Magic Eden: сбой проверки коллекции {symbol}: {exc}") from exc
        return {"symbol": data.get("symbol", symbol), "listed_count": data.get("listedCount")}
