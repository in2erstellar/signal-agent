"""Helius client for the first SOL funding transfer and top token holders.

The default lookup pages standard RPC ``getSignaturesForAddress`` (newest
first; ``before`` walks toward older signatures) and reads the oldest
signature with ``getTransaction``. ``GET /v1/wallet/{wallet}/funded-by``
stays available behind ``FUNDER_LOOKUP_METHOD = helius_funded_by``. That
Wallet API call is paid: a free-plan key receives HTTP 403.

Top holders come from RPC ``getTokenLargestAccounts`` (at most 20 token
accounts) plus ``getMultipleAccounts`` to read each account's owner.
``getTokenLargestAccounts`` returns token accounts, not wallets.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from typing import Any, cast

import httpx
import structlog

from app.config import (
    FUNDER_LOOKUP_METHOD,
    MAX_FUNDING_LOOKUP_SIGNATURES,
    MAX_FUNDING_TRANSACTION_INSPECT,
    Settings,
)

logger = structlog.get_logger(__name__)

REQUEST_TIMEOUT_SECONDS = 10.0
MAX_ATTEMPTS = 3
# Solana RPC returns at most this many largest token accounts.
MAX_LARGEST_TOKEN_ACCOUNTS = 20
# Solana RPC rejects a getSignaturesForAddress limit above this.
_SIGNATURE_PAGE_LIMIT = 1000
_LAMPORTS_PER_SOL = Decimal(1_000_000_000)
_RETRYABLE_STATUS = frozenset({408, 500, 502, 503, 504})

Sleep = Callable[[float], Awaitable[None]]


class HeliusError(Exception):
    """The Helius request failed after retries, or the response was unusable."""


class HeliusRateLimitError(HeliusError):
    """HTTP 429 persisted for every attempt."""


class HeliusResponseError(HeliusError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


class FundingDepthExceeded(HeliusError):
    """The wallet's history is longer than the signature cap.

    This is not "the wallet was never funded". The oldest transfer was not
    in the pages we were willing to read.
    """

    def __init__(self, wallet_address: str, scanned: int) -> None:
        self.wallet_address = wallet_address
        self.scanned = scanned
        super().__init__(
            f"funding history for {wallet_address} exceeded {scanned} signatures; funder unknown"
        )


class FundingSourceUnresolved(HeliusError):
    """The oldest transaction did not show a positive SOL inflow.

    Distinct from an empty history, which means the address has no transactions.
    """

    def __init__(self, wallet_address: str, signature: str, reason: str) -> None:
        self.wallet_address = wallet_address
        self.signature = signature
        super().__init__(
            f"funding source for {wallet_address} was not in transaction {signature}: {reason}"
        )


@dataclass(frozen=True)
class WebhookConfig:
    """One Helius webhook. ``account_addresses`` is the monitored list.

    PUT replaces the webhook configuration, so an address edit sends these
    fields back. The shape is the 2026 Webhooks API:
    https://www.helius.dev/docs/api-reference/webhooks/llms.txt
    """

    webhook_id: str
    webhook_url: str
    webhook_type: str
    account_addresses: tuple[str, ...]
    transaction_types: tuple[str, ...]
    auth_header: str | None = None
    txn_status: str | None = None


@dataclass(frozen=True)
class FundingInfo:
    """Earliest incoming SOL transfer into ``wallet_address``."""

    wallet_address: str
    funder: str
    amount_sol: Decimal
    signature: str
    funded_at: datetime
    slot: int | None


class HeliusClient:
    """Async Helius client. Pass ``sleep`` in tests so backoff is not real time."""

    def __init__(
        self,
        *,
        api_key: str,
        api_base_url: str,
        rpc_url: str,
        client: httpx.AsyncClient | None = None,
        timeout_seconds: float = REQUEST_TIMEOUT_SECONDS,
        max_attempts: int = MAX_ATTEMPTS,
        sleep: Sleep = asyncio.sleep,
        funder_lookup_method: str = FUNDER_LOOKUP_METHOD,
        signature_page_size: int = _SIGNATURE_PAGE_LIMIT,
        max_funding_signatures: int = MAX_FUNDING_LOOKUP_SIGNATURES,
        max_funding_transactions: int = MAX_FUNDING_TRANSACTION_INSPECT,
    ) -> None:
        if not api_key:
            raise ValueError("HELIUS_API_KEY is empty")
        if funder_lookup_method not in {"rpc", "helius_funded_by"}:
            raise ValueError("funder_lookup_method must be rpc or helius_funded_by")
        if not 1 <= signature_page_size <= _SIGNATURE_PAGE_LIMIT:
            raise ValueError(f"signature_page_size must be 1..{_SIGNATURE_PAGE_LIMIT}")
        if max_funding_signatures < 1:
            raise ValueError("max_funding_signatures must be at least 1")
        if max_funding_transactions < 1:
            raise ValueError("max_funding_transactions must be at least 1")
        self._api_key = api_key
        self._api_base_url = api_base_url.rstrip("/")
        self._rpc_url = rpc_url.rstrip("/")
        self._timeout = httpx.Timeout(timeout_seconds)
        self._max_attempts = max_attempts
        self._sleep = sleep
        self._funder_lookup_method = funder_lookup_method
        self._signature_page_size = signature_page_size
        self._max_funding_signatures = max_funding_signatures
        self._max_funding_transactions = max_funding_transactions
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=self._timeout)
        # One in-flight HTTP call. The scan semaphore is wider; this lock
        # keeps a 429 from becoming three simultaneous RPCs.
        self._send_lock = asyncio.Lock()
        self._pause_cv = asyncio.Condition()
        self._paused_until = 0.0
        self._now = time.monotonic

    @classmethod
    def from_settings(cls, settings: Settings, **kwargs: Any) -> HeliusClient:
        return cls(
            api_key=settings.helius_api_key.get_secret_value(),
            api_base_url=settings.helius_api_base_url,
            rpc_url=settings.helius_rpc_url,
            **kwargs,
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> HeliusClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def get_webhook(self, webhook_id: str) -> WebhookConfig:
        """GET /v0/webhooks/{webhookID}?api-key= on the mainnet RPC host."""
        response = await self._request(
            "GET",
            self._webhook_url(webhook_id),
            params={"api-key": self._api_key},
        )
        if response.status_code != 200:
            raise HeliusResponseError(
                response.status_code,
                f"get webhook failed with HTTP {response.status_code}",
            )
        return _parse_webhook(response.json())

    async def add_address_to_webhook(self, webhook_id: str, address: str) -> bool:
        """Append ``address`` to accountAddresses. Already present is a no-op."""
        current = await self.get_webhook(webhook_id)
        if address in current.account_addresses:
            return True
        updated = WebhookConfig(
            webhook_id=current.webhook_id,
            webhook_url=current.webhook_url,
            webhook_type=current.webhook_type,
            account_addresses=(*current.account_addresses, address),
            transaction_types=current.transaction_types,
            auth_header=current.auth_header,
            txn_status=current.txn_status,
        )
        saved = await self._put_webhook(updated)
        if address not in saved.account_addresses:
            raise HeliusResponseError(200, "webhook update did not store the address")
        return True

    async def remove_address_from_webhook(self, webhook_id: str, address: str) -> bool:
        """Drop ``address`` from accountAddresses. Already absent is a no-op."""
        current = await self.get_webhook(webhook_id)
        if address not in current.account_addresses:
            return True
        updated = WebhookConfig(
            webhook_id=current.webhook_id,
            webhook_url=current.webhook_url,
            webhook_type=current.webhook_type,
            account_addresses=tuple(
                item for item in current.account_addresses if item != address
            ),
            transaction_types=current.transaction_types,
            auth_header=current.auth_header,
            txn_status=current.txn_status,
        )
        saved = await self._put_webhook(updated)
        if address in saved.account_addresses:
            raise HeliusResponseError(200, "webhook update did not remove the address")
        return True

    def _webhook_url(self, webhook_id: str) -> str:
        return f"{self._rpc_url}/v0/webhooks/{webhook_id}"

    async def _put_webhook(self, config: WebhookConfig) -> WebhookConfig:
        body: dict[str, Any] = {
            "webhookURL": config.webhook_url,
            "webhookType": config.webhook_type,
            "accountAddresses": list(config.account_addresses),
            "transactionTypes": list(config.transaction_types),
        }
        if config.auth_header is not None:
            body["authHeader"] = config.auth_header
        if config.txn_status is not None:
            body["txnStatus"] = config.txn_status
        response = await self._request(
            "PUT",
            self._webhook_url(config.webhook_id),
            params={"api-key": self._api_key},
            json=body,
        )
        if response.status_code != 200:
            raise HeliusResponseError(
                response.status_code,
                f"update webhook failed with HTTP {response.status_code}",
            )
        return _parse_webhook(response.json())

    async def get_first_funding_tx(self, wallet_address: str) -> FundingInfo | None:
        """Paid Wallet API. HTTP 404 means this address never received SOL.

        Kept so ``FUNDER_LOOKUP_METHOD = helius_funded_by`` can call it without
        a second implementation. Free-plan keys receive HTTP 403.
        """
        response = await self._request(
            "GET",
            f"{self._api_base_url}/v1/wallet/{wallet_address}/funded-by",
            headers={"X-Api-Key": self._api_key},
        )
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise HeliusResponseError(
                response.status_code,
                f"funded-by failed for {wallet_address} with HTTP {response.status_code}",
            )
        return _parse_funding(wallet_address, response.json())

    async def find_first_funding_tx(self, wallet_address: str) -> FundingInfo | None:
        """First incoming SOL transfer, or None when the address has no transactions.

        ``rpc`` pages ``getSignaturesForAddress`` (newest first) to the oldest
        signature, then one ``getTransaction``. Hitting
        ``max_funding_signatures`` on a full page raises
        ``FundingDepthExceeded`` instead of returning None. The paid path
        delegates to ``get_first_funding_tx``.
        """
        if self._funder_lookup_method == "helius_funded_by":
            return await self.get_first_funding_tx(wallet_address)
        oldest_first, scanned = await self._signatures_oldest_first(wallet_address)
        if not oldest_first:
            logger.info(
                "funding_lookup_no_history",
                wallet=wallet_address,
                signatures_scanned=scanned,
            )
            return None
        successful = [row for row in oldest_first if row.get("err") is None]
        if not successful:
            logger.info(
                "funding_lookup_no_history",
                wallet=wallet_address,
                signatures_scanned=scanned,
            )
            return None
        inspected = 0
        last_signature = str(successful[0]["signature"])
        for row in successful[: self._max_funding_transactions]:
            signature = str(row["signature"])
            last_signature = signature
            inspected += 1
            transaction = await self._call_rpc(
                "getTransaction",
                [
                    signature,
                    {
                        "encoding": "jsonParsed",
                        "commitment": "confirmed",
                        "maxSupportedTransactionVersion": 0,
                    },
                ],
            )
            if not isinstance(transaction, dict):
                continue
            info = funding_from_transaction(
                wallet_address,
                signature,
                transaction,
                fallback_block_time=row.get("blockTime"),
            )
            if info is None:
                continue
            logger.info(
                "funding_lookup_rpc",
                wallet=wallet_address,
                funder=info.funder,
                signatures_scanned=scanned,
                transactions_inspected=inspected,
                signature=signature,
            )
            return info
        if inspected >= len(successful):
            # Every successful signature was read. None brought SOL in.
            logger.info(
                "funding_lookup_no_sol_inflow",
                wallet=wallet_address,
                signatures_scanned=scanned,
                transactions_inspected=inspected,
            )
            return None
        raise FundingSourceUnresolved(
            wallet_address,
            last_signature,
            "no positive SOL inflow in the oldest inspected transactions",
        )

    async def _signatures_oldest_first(
        self,
        wallet_address: str,
    ) -> tuple[list[Mapping[str, Any]], int]:
        """Last signature after walking newest-first pages, and how many were read.

        Solana returns each page from newest to oldest. ``before`` excludes
        the signature you pass and continues toward older ones. A short page
        is the end of history. A full page at the cap is not the first
        transaction.
        """
        collected: list[Mapping[str, Any]] = []
        before: str | None = None
        while len(collected) < self._max_funding_signatures:
            limit = min(self._signature_page_size, self._max_funding_signatures - len(collected))
            config: dict[str, Any] = {"limit": limit, "commitment": "confirmed"}
            if before is not None:
                config["before"] = before
            result = await self._call_rpc(
                "getSignaturesForAddress",
                [wallet_address, config],
            )
            page = _signature_rows(result)
            if not page:
                break
            collected.extend(page)
            if len(page) < limit:
                break
            if len(collected) >= self._max_funding_signatures:
                logger.warning(
                    "funding_lookup_depth_exceeded",
                    wallet=wallet_address,
                    signatures_scanned=len(collected),
                    limit=self._max_funding_signatures,
                )
                raise FundingDepthExceeded(wallet_address, len(collected))
            before = str(page[-1]["signature"])
        if not collected:
            return [], 0
        return list(reversed(collected)), len(collected)

    async def get_top_holder_owners(self, mint: str, top_n: int) -> list[str]:
        """Owner wallets of the largest token accounts, largest balance first.

        ``top_n`` cannot exceed 20: that is the RPC cap for
        ``getTokenLargestAccounts``. Asking for every holder would mean paging
        ``getTokenAccounts``, which this method does not do.
        """
        if not 1 <= top_n <= MAX_LARGEST_TOKEN_ACCOUNTS:
            raise ValueError(
                f"top_n must be between 1 and {MAX_LARGEST_TOKEN_ACCOUNTS}"
            )
        largest = await self._rpc("getTokenLargestAccounts", [mint])
        accounts = list(largest.get("value") or [])[:top_n]
        token_accounts = [str(item["address"]) for item in accounts if item.get("address")]
        if not token_accounts:
            return []
        multiple = await self._rpc(
            "getMultipleAccounts",
            [token_accounts, {"encoding": "jsonParsed"}],
        )
        return parse_holder_owners(token_accounts, list(multiple.get("value") or []))

    async def rpc(self, method: str, params: list[Any]) -> dict[str, Any]:
        """JSON-RPC result object. Retries and timeouts match the rest of the client."""
        return await self._rpc(method, params)

    async def _rpc(self, method: str, params: list[Any]) -> dict[str, Any]:
        result = await self._call_rpc(method, params)
        if not isinstance(result, dict):
            raise HeliusResponseError(200, f"{method}: missing result")
        return result

    async def _call_rpc(self, method: str, params: list[Any]) -> Any:
        """JSON-RPC result, including a list or null. ``getSignaturesForAddress`` is a list."""
        response = await self._request(
            "POST",
            self._rpc_url,
            params={"api-key": self._api_key},
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        )
        if response.status_code != 200:
            raise HeliusResponseError(
                response.status_code,
                f"Helius RPC {method} failed with HTTP {response.status_code}",
            )
        body = response.json()
        if isinstance(body, dict) and body.get("error"):
            error = body["error"]
            message = error.get("message", "RPC error") if isinstance(error, dict) else str(error)
            raise HeliusResponseError(response.status_code, f"{method}: {message}")
        if not isinstance(body, dict) or "result" not in body:
            raise HeliusResponseError(response.status_code, f"{method}: missing result")
        return body["result"]

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Try up to ``max_attempts`` times. One 429 pauses every caller.

        Order on 429: record the shared deadline and notify, leave the send
        lock, then wait. Waiting while the lock is held would wake the other
        scans one by one and each would send into the same limited window.
        """
        last_status: int | None = None
        attempt = 0
        while attempt < self._max_attempts:
            await self._wait_until_open()
            async with self._send_lock:
                if self._now() < self._paused_until:
                    deferred = True
                    response: httpx.Response | None = None
                    transport_error: BaseException | None = None
                else:
                    deferred = False
                    try:
                        response = await self._client.request(
                            method,
                            url,
                            timeout=self._timeout,
                            **kwargs,
                        )
                        transport_error = None
                    except (httpx.TimeoutException, httpx.TransportError) as exc:
                        response = None
                        transport_error = exc
                    else:
                        if response.status_code == 429:
                            delay = _retry_delay(response.headers, attempt)
                            logger.warning(
                                "helius_rate_limited",
                                method=method,
                                attempt=attempt + 1,
                                retry_in_seconds=delay,
                            )
                            await response.aclose()
                            # Notify under the condition, then drop the send
                            # lock before any caller waits on the deadline.
                            await self._extend_pause(delay)
            if deferred:
                await self._wait_until_open()
                continue
            assert response is not None or transport_error is not None
            if transport_error is not None:
                logger.warning(
                    "helius_transport_error",
                    method=method,
                    attempt=attempt + 1,
                    error_type=type(transport_error).__name__,
                )
                if attempt + 1 >= self._max_attempts:
                    raise HeliusError(
                        f"Helius {method} failed after {self._max_attempts} attempts"
                    ) from transport_error
                attempt += 1
                await self._sleep(_backoff_seconds(attempt - 1))
                continue
            assert response is not None
            last_status = response.status_code
            if response.status_code == 429:
                if attempt + 1 >= self._max_attempts:
                    raise HeliusRateLimitError(
                        f"Helius rate limited {method} after {self._max_attempts} attempts"
                    )
                attempt += 1
                await self._wait_until_open()
                continue
            if response.status_code in _RETRYABLE_STATUS:
                logger.warning(
                    "helius_retryable_status",
                    method=method,
                    status_code=response.status_code,
                    attempt=attempt + 1,
                )
                await response.aclose()
                if attempt + 1 >= self._max_attempts:
                    break
                attempt += 1
                await self._sleep(_backoff_seconds(attempt - 1))
                continue
            return response

        raise HeliusResponseError(
            last_status or 0,
            f"Helius {method} failed with HTTP {last_status}",
        )

    async def _extend_pause(self, delay: float) -> None:
        """Push the shared open time out. A shorter delay does not pull it in."""
        async with self._pause_cv:
            until = self._now() + max(0.0, delay)
            if until > self._paused_until:
                self._paused_until = until
            self._pause_cv.notify_all()

    async def _wait_until_open(self) -> None:
        """Sleep until the shared 429 deadline. Concurrent callers share it."""
        while True:
            async with self._pause_cv:
                remaining = self._paused_until - self._now()
                if remaining <= 0:
                    return
                deadline = self._paused_until
            await self._sleep(remaining)
            async with self._pause_cv:
                # A test sleep returns without moving the clock. Real time
                # does move, so this only clears a deadline the sleep already
                # covered. A longer deadline armed in between is left alone.
                if self._paused_until == deadline and self._now() < deadline:
                    self._paused_until = self._now()
                    self._pause_cv.notify_all()


def _parse_webhook(payload: Any) -> WebhookConfig:
    if not isinstance(payload, dict):
        raise HeliusResponseError(200, "webhook payload is not an object")
    webhook_id = payload.get("webhookID")
    webhook_url = payload.get("webhookURL")
    webhook_type = payload.get("webhookType")
    if not isinstance(webhook_id, str) or not webhook_id:
        raise HeliusResponseError(200, "webhook payload has no webhookID")
    if not isinstance(webhook_url, str) or not webhook_url:
        raise HeliusResponseError(200, "webhook payload has no webhookURL")
    if not isinstance(webhook_type, str) or not webhook_type:
        raise HeliusResponseError(200, "webhook payload has no webhookType")
    addresses = payload.get("accountAddresses") or []
    types = payload.get("transactionTypes") or []
    if not isinstance(addresses, list) or not isinstance(types, list):
        raise HeliusResponseError(200, "webhook address list is not an array")
    auth = payload.get("authHeader")
    status = payload.get("txnStatus")
    return WebhookConfig(
        webhook_id=webhook_id,
        webhook_url=webhook_url,
        webhook_type=webhook_type,
        account_addresses=tuple(item for item in addresses if isinstance(item, str)),
        transaction_types=tuple(item for item in types if isinstance(item, str)),
        auth_header=auth if isinstance(auth, str) else None,
        txn_status=status if isinstance(status, str) else None,
    )


def parse_holder_owners(
    token_accounts: Sequence[str],
    account_infos: Sequence[Mapping[str, Any] | None],
) -> list[str]:
    """Map largest token accounts to unique owner wallets, preserving balance order."""
    owners: list[str] = []
    seen: set[str] = set()
    for info in account_infos:
        owner = _owner_from_account(info)
        if owner is None or owner in seen:
            continue
        seen.add(owner)
        owners.append(owner)
    if len(account_infos) != len(token_accounts):
        logger.warning(
            "helius_holder_account_mismatch",
            requested=len(token_accounts),
            returned=len(account_infos),
        )
    return owners


def _owner_from_account(info: Mapping[str, Any] | None) -> str | None:
    if not info:
        return None
    data = info.get("data")
    if not isinstance(data, dict):
        return None
    parsed = data.get("parsed")
    if not isinstance(parsed, dict):
        return None
    account_info = parsed.get("info")
    if not isinstance(account_info, dict):
        return None
    owner = account_info.get("owner")
    if not isinstance(owner, str) or not owner:
        return None
    return owner


def _signature_rows(result: Any) -> list[Mapping[str, Any]]:
    if result is None:
        return []
    if not isinstance(result, list):
        raise HeliusResponseError(200, "getSignaturesForAddress: result is not a list")
    rows: list[Mapping[str, Any]] = []
    for item in result:
        if isinstance(item, dict) and isinstance(item.get("signature"), str) and item["signature"]:
            rows.append(item)
    return rows


def funding_from_transaction(
    wallet_address: str,
    signature: str,
    transaction: Mapping[str, Any],
    *,
    fallback_block_time: Any = None,
) -> FundingInfo | None:
    """Sender and lamport gain for ``wallet_address`` in one ``getTransaction`` result.

    None means this transaction did not increase the wallet's SOL balance.
    The caller decides whether that is an empty history or an unresolved oldest tx.
    """
    meta = transaction.get("meta")
    if not isinstance(meta, dict) or meta.get("err") is not None:
        return None
    keys = _transaction_account_keys(transaction)
    try:
        index = keys.index(wallet_address)
    except ValueError:
        return None
    pre = _lamport_list(meta.get("preBalances"))
    post = _lamport_list(meta.get("postBalances"))
    if index >= len(pre) or index >= len(post):
        return None
    gain = post[index] - pre[index]
    if gain <= 0:
        return None
    fee = _lamports(meta.get("fee"))
    funder = _sol_sender(keys, pre, post, index, gain, fee)
    if funder is None or funder == wallet_address:
        return None
    funded_at = _tx_time(transaction.get("blockTime"), fallback_block_time)
    if funded_at is None:
        return None
    slot_raw = transaction.get("slot")
    slot = int(slot_raw) if isinstance(slot_raw, int) else None
    return FundingInfo(
        wallet_address=wallet_address,
        funder=funder,
        amount_sol=Decimal(gain) / _LAMPORTS_PER_SOL,
        signature=signature,
        funded_at=funded_at,
        slot=slot,
    )


def _transaction_account_keys(transaction: Mapping[str, Any]) -> list[str]:
    message = transaction.get("transaction")
    if not isinstance(message, dict):
        return []
    inner = message.get("message")
    if not isinstance(inner, dict):
        return []
    keys: list[str] = []
    raw_keys = inner.get("accountKeys")
    if isinstance(raw_keys, list):
        for key in raw_keys:
            if isinstance(key, str) and key:
                keys.append(key)
            elif isinstance(key, dict) and isinstance(key.get("pubkey"), str):
                keys.append(str(key["pubkey"]))
    meta = transaction.get("meta")
    loaded = meta.get("loadedAddresses") if isinstance(meta, dict) else None
    if isinstance(loaded, dict):
        for bucket in ("writable", "readonly"):
            extra = loaded.get(bucket)
            if isinstance(extra, list):
                keys.extend(item for item in extra if isinstance(item, str) and item)
    return keys


def _lamport_list(value: Any) -> list[int]:
    if not isinstance(value, list):
        return []
    return [_lamports(item) for item in value]


def _lamports(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return cast(int, value)


def _sol_sender(
    keys: Sequence[str],
    pre: Sequence[int],
    post: Sequence[int],
    wallet_index: int,
    gain: int,
    fee: int,
) -> str | None:
    """Account whose SOL drop matches the wallet's gain, fee included when they paid it."""
    exact: list[str] = []
    largest_drop = 0
    largest_key: str | None = None
    for index, key in enumerate(keys):
        if index == wallet_index or index >= len(pre) or index >= len(post):
            continue
        drop = pre[index] - post[index]
        if drop <= 0:
            continue
        if drop > largest_drop:
            largest_drop = drop
            largest_key = key
        if drop == gain + fee or drop == gain:
            exact.append(key)
    if len(exact) == 1:
        return exact[0]
    if exact:
        return exact[0]
    return largest_key


def _tx_time(block_time: Any, fallback: Any) -> datetime | None:
    for value in (block_time, fallback):
        if isinstance(value, int) and not isinstance(value, bool):
            return datetime.fromtimestamp(value, tz=timezone.utc)
    return None


def _parse_funding(wallet_address: str, payload: Any) -> FundingInfo:
    if not isinstance(payload, dict) or not payload.get("funder") or not payload.get("signature"):
        raise HeliusResponseError(200, f"funded-by payload for {wallet_address} is incomplete")
    return FundingInfo(
        wallet_address=wallet_address,
        funder=str(payload["funder"]),
        amount_sol=_amount_sol(payload),
        signature=str(payload["signature"]),
        funded_at=_funded_at(payload),
        slot=int(payload["slot"]) if payload.get("slot") is not None else None,
    )


def _amount_sol(payload: Mapping[str, Any]) -> Decimal:
    decimals = int(payload.get("decimals") or 9)
    raw = payload.get("amountRaw")
    if raw is not None:
        return Decimal(str(raw)) / (Decimal(10) ** decimals)
    if payload.get("amount") is None:
        raise HeliusResponseError(200, "funded-by payload has no amount")
    return Decimal(str(payload["amount"]))


def _funded_at(payload: Mapping[str, Any]) -> datetime:
    if payload.get("timestamp") is not None:
        return datetime.fromtimestamp(int(payload["timestamp"]), tz=timezone.utc)
    date = payload.get("date")
    if isinstance(date, str):
        return datetime.fromisoformat(date.replace("Z", "+00:00"))
    raise HeliusResponseError(200, "funded-by payload has no timestamp")


def _backoff_seconds(attempt: float) -> float:
    """Wait 1s, then 2s, then 4s. ``attempt`` is the zero-based attempt that just failed."""
    return float(2**attempt)


def _retry_delay(headers: httpx.Headers, attempt: int) -> float:
    raw = headers.get("Retry-After")
    if raw:
        try:
            return max(0.0, float(raw))
        except ValueError:
            parsed_raw = parsedate_to_datetime(raw)
            if not isinstance(parsed_raw, datetime):
                return _backoff_seconds(attempt)
            if parsed_raw.tzinfo is None:
                parsed_raw = parsed_raw.replace(tzinfo=timezone.utc)
            return max(0.0, (parsed_raw - datetime.now(timezone.utc)).total_seconds())
    return _backoff_seconds(attempt)
