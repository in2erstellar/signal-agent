"""Helius client for the first SOL funding transfer and top token holders.

The first incoming SOL transfer is ``GET /v1/wallet/{wallet}/funded-by``
on the Wallet API (https://www.helius.dev/docs/wallet-api/funded-by).
One response is the funding transaction. The enhanced address history
(``/v0/addresses/{address}/transactions``) is newest-first and would require
paging the whole history, so this client does not use it.

Top holders come from RPC ``getTokenLargestAccounts`` (at most 20 token
accounts) plus ``getMultipleAccounts`` to read each account's owner.
``getTokenLargestAccounts`` returns token accounts, not wallets.

``funded-by`` is a paid-plan endpoint. A free-plan key receives HTTP 403.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
import structlog

from app.config import Settings

logger = structlog.get_logger(__name__)

REQUEST_TIMEOUT_SECONDS = 10.0
MAX_ATTEMPTS = 3
# Solana RPC returns at most this many largest token accounts.
MAX_LARGEST_TOKEN_ACCOUNTS = 20
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
    ) -> None:
        if not api_key:
            raise ValueError("HELIUS_API_KEY is empty")
        self._api_key = api_key
        self._api_base_url = api_base_url.rstrip("/")
        self._rpc_url = rpc_url.rstrip("/")
        self._timeout = httpx.Timeout(timeout_seconds)
        self._max_attempts = max_attempts
        self._sleep = sleep
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=self._timeout)

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
        """Return the wallet's first incoming SOL transfer, or None if it never received SOL."""
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
            message = body["error"].get("message", "RPC error")
            raise HeliusResponseError(response.status_code, f"{method}: {message}")
        result = body.get("result") if isinstance(body, dict) else None
        if not isinstance(result, dict):
            raise HeliusResponseError(response.status_code, f"{method}: missing result")
        return result

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Try up to ``max_attempts`` times. 429 waits for Retry-After when present."""
        last_status: int | None = None
        for attempt in range(self._max_attempts):
            try:
                response = await self._client.request(
                    method,
                    url,
                    timeout=self._timeout,
                    **kwargs,
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                logger.warning(
                    "helius_transport_error",
                    method=method,
                    attempt=attempt + 1,
                    error_type=type(exc).__name__,
                )
                if attempt + 1 >= self._max_attempts:
                    raise HeliusError(
                        f"Helius {method} failed after {self._max_attempts} attempts"
                    ) from exc
                await self._sleep(_backoff_seconds(attempt))
                continue

            last_status = response.status_code
            if response.status_code == 429:
                if attempt + 1 >= self._max_attempts:
                    raise HeliusRateLimitError(
                        f"Helius rate limited {method} after {self._max_attempts} attempts"
                    )
                delay = _retry_delay(response.headers, attempt)
                logger.warning(
                    "helius_rate_limited",
                    method=method,
                    attempt=attempt + 1,
                    retry_in_seconds=delay,
                )
                await response.aclose()
                await self._sleep(delay)
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
                await self._sleep(_backoff_seconds(attempt))
                continue
            return response

        raise HeliusResponseError(
            last_status or 0,
            f"Helius {method} failed with HTTP {last_status}",
        )


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
