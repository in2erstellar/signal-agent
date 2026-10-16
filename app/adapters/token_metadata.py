"""Off-chain token metadata (the JSON behind ``tokenMetadata.uri``).

Pump.fun points the URI at IPFS. Only IPFS URIs are fetched, and only through
the configured gateway: the URI is creator-controlled, so an arbitrary host
is never requested. Anything else reads as unsupported, not as "no socials".
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import httpx

REQUEST_TIMEOUT_SECONDS = 8.0
SOCIAL_FIELDS = ("twitter", "telegram", "website")

# CIDv0 (Qm...) or CIDv1 base32 (b...), optionally followed by a path.
_CID = r"(Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{50,})"
_IPFS_PATTERNS = (
    re.compile(rf"^ipfs://(?:ipfs/)?{_CID}(/[\w.\-/]*)?$"),
    re.compile(rf"^https://[\w.\-]+/ipfs/{_CID}(/[\w.\-/]*)?$"),
)


class TokenMetadataError(Exception):
    """The gateway did not return a usable metadata JSON."""


@dataclass(frozen=True)
class MetadataSocials:
    """Non-empty social links as the creator wrote them. Nobody verified them."""

    links: dict[str, str]


def ipfs_path(uri: str) -> str | None:
    """``<cid>[/path]`` for an IPFS URI, or ``None`` for any other URI."""
    for pattern in _IPFS_PATTERNS:
        match = pattern.match(uri.strip())
        if match:
            return match.group(1) + (match.group(2) or "")
    return None


class TokenMetadataClient:
    def __init__(
        self,
        gateway: str,
        *,
        client: httpx.AsyncClient | None = None,
        timeout_seconds: float = REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        self._gateway = gateway.rstrip("/")
        self._timeout = httpx.Timeout(timeout_seconds)
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=self._timeout)

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def socials(self, uri: str) -> MetadataSocials | None:
        """``None`` when the URI is not IPFS. Fetch failures raise ``TokenMetadataError``."""
        path = ipfs_path(uri)
        if path is None:
            return None
        try:
            response = await self._client.get(
                f"{self._gateway}/{path}",
                timeout=self._timeout,
                follow_redirects=False,
            )
        except httpx.HTTPError as exc:
            raise TokenMetadataError(f"metadata request failed: {type(exc).__name__}") from exc
        if response.status_code != 200:
            raise TokenMetadataError(f"metadata lookup failed with HTTP {response.status_code}")
        try:
            payload = response.json()
        except ValueError as exc:
            raise TokenMetadataError("metadata is not JSON") from exc
        return MetadataSocials(links=_links(payload))


def _links(payload: Any) -> dict[str, str]:
    if not isinstance(payload, dict):
        raise TokenMetadataError("metadata JSON is not an object")
    links: dict[str, str] = {}
    for field in SOCIAL_FIELDS:
        value = payload.get(field)
        if isinstance(value, str) and value.strip():
            links[field] = value.strip()
    return links
