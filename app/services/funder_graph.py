"""Sybil-кластеры по общему источнику первого SOL.

Разработчик, который режет supply на много кошельков, обычно кормит их
из одного источника: напрямую (hop 1) или через один промежуточный
кошелёк (hop 2). Модуль это и ищет.

Запросы к Helius идут по очереди. Параллельный фан-аут быстрее упирается
в 429, а Retry-After тогда уже не про одну попытку, а про пачку.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Protocol

import structlog
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.adapters.helius import (
    FundingDepthExceeded,
    FundingInfo,
    FundingSourceUnresolved,
    HeliusError,
    HeliusRateLimitError,
    HeliusResponseError,
)
from app.db.models import FunderCluster, Token, WalletFunder

logger = structlog.get_logger(__name__)


class FunderLookupFailure(Exception):
    """One wallet's funded-by call failed. The queue decides whether to retry.

    ``permanent`` is true for a plan or client error (HTTP 403 and other 4xx
    except 429). Retrying those returns the same status. Timeouts and 429 are
    not permanent: the worker counts them per wallet and per status.
    """

    def __init__(
        self,
        wallet_address: str,
        *,
        status_code: int | None,
        permanent: bool,
        message: str,
    ) -> None:
        super().__init__(message)
        self.wallet_address = wallet_address
        self.status_code = status_code
        self.permanent = permanent


def funder_lookup_failure(wallet_address: str, exc: Exception) -> FunderLookupFailure | None:
    """Classify a Helius error from a funding lookup. Other errors stay unwrapped."""
    if isinstance(exc, FundingDepthExceeded | FundingSourceUnresolved):
        # Same depth or the same oldest transaction will not become a funder on retry.
        return FunderLookupFailure(
            wallet_address,
            status_code=None,
            permanent=True,
            message=str(exc),
        )
    if isinstance(exc, HeliusRateLimitError):
        return FunderLookupFailure(
            wallet_address,
            status_code=429,
            permanent=False,
            message=str(exc),
        )
    if isinstance(exc, HeliusResponseError):
        status = exc.status_code
        permanent = status == 403 or (400 <= status < 500 and status != 429)
        return FunderLookupFailure(
            wallet_address,
            status_code=status,
            permanent=permanent,
            message=str(exc),
        )
    if isinstance(exc, HeliusError):
        return FunderLookupFailure(
            wallet_address,
            status_code=None,
            permanent=False,
            message=str(exc),
        )
    return None


# The RPC refuses getTokenLargestAccounts for a mint whose holder set is too
# large to scan. The same body comes back on every attempt.
_HOLDER_LIST_PERMANENT_MARKERS = (
    "too many accounts",
    "too many pubkeys",
)


def holder_list_failure(mint: str, exc: Exception) -> FunderLookupFailure | None:
    """Classify a Helius error from ``get_top_holder_owners``.

    A holder-count refusal is permanent on the first attempt: retrying
    ``getTokenLargestAccounts`` cannot shrink the mint. Every other Helius
    error uses the funded-by rules, so a timeout or 429 is counted and a 4xx
    is permanent. Anything that is not a Helius error stays unwrapped.
    """
    failure = funder_lookup_failure(mint, exc)
    if failure is None:
        return None
    if any(marker in str(exc).lower() for marker in _HOLDER_LIST_PERMANENT_MARKERS):
        return FunderLookupFailure(
            mint,
            status_code=failure.status_code,
            permanent=True,
            message=str(exc),
        )
    return failure


class FundingLookup(Protocol):
    """Что билдеру нужно от Helius. Живой клиент и тестовый двойник оба это закрывают."""

    async def get_top_holder_owners(self, mint: str, top_n: int) -> list[str]: ...

    async def find_first_funding_tx(self, wallet_address: str) -> FundingInfo | None: ...


# Меньше трёх холдеров — слишком слабый сигнал: общий фандер бывает и у пары
# обычных кошельков. Порог зафиксирован задачей.
MIN_CLUSTER_SIZE = 3


class DisjointSet:
    """Непересекающиеся множества адресов (union-find).

    Каждое множество — кандидаты в один sybil-кластер. Операции почти O(1)
    за счёт двух классических оптимизаций:

    * path compression в ``find``: после поиска корня каждый узел на пути
      начинает ссылаться прямо на корень, следующая проверка короче;
    * union by rank: меньшее дерево подвешиваем к большему, чтобы цепочки
      не вытягивались в список.
    """

    def __init__(self, items: list[str]) -> None:
        # Родитель каждого узла. Пока все сами себе корни — каждый холдер
        # в своём кластере, связей ещё нет.
        self._parent = {item: item for item in items}
        self._rank = {item: 0 for item in items}

    def find(self, item: str) -> str:
        parent = self._parent[item]
        if parent != item:
            # Сжимаем путь: дед становится родителем, корень ищем рекурсивно.
            self._parent[item] = self.find(parent)
        return self._parent[item]

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        left_rank = self._rank[left_root]
        right_rank = self._rank[right_root]
        if left_rank < right_rank:
            self._parent[left_root] = right_root
        elif left_rank > right_rank:
            self._parent[right_root] = left_root
        else:
            self._parent[right_root] = left_root
            self._rank[left_root] = left_rank + 1

    def same(self, left: str, right: str) -> bool:
        return self.find(left) == self.find(right)

    def components(self) -> list[list[str]]:
        grouped: dict[str, list[str]] = defaultdict(list)
        for item in self._parent:
            grouped[self.find(item)].append(item)
        return [sorted(members) for members in grouped.values()]


@dataclass(frozen=True)
class HolderCluster:
    root_funder: str
    members: tuple[str, ...]


def cluster_holders(
    holders: list[str],
    hop1: dict[str, str],
    hop2: dict[str, str],
) -> list[HolderCluster]:
    """Сгруппировать холдеров по общим фандерам hop=1 или hop=2.

    Для холдера H известны:
    * F1 — кто прислал ему первый SOL (прямая связь);
    * F2 — кто прислал первый SOL самому F1 (корень через один hop).

    Два холдера объединяются, если у них совпал F1 или совпал F2.
    Совпадение F2 как раз ловит схему «один корень, разные промежуточные
    кошельки»: на hop=1 фандеры разные, на hop=2 — один и тот же.

    В результат попадают только компоненты из ``MIN_CLUSTER_SIZE`` холдеров
    и больше. Пара с общим фандером кластером не считается.
    """
    dsu = DisjointSet(holders)
    # Первый холдер, уже привязанный к этому адресу фандера.
    # Следующий холдер с тем же адресом сливается с ним.
    attached: dict[str, str] = {}
    for holder in holders:
        funder = hop1.get(holder)
        if funder is None or funder == holder:
            continue
        ancestors = [funder]
        root = hop2.get(funder)
        if root is not None and root not in {holder, funder}:
            ancestors.append(root)
        for address in ancestors:
            previous = attached.get(address)
            if previous is None:
                attached[address] = holder
            else:
                dsu.union(holder, previous)

    clusters: list[HolderCluster] = []
    for members in dsu.components():
        if len(members) < MIN_CLUSTER_SIZE:
            continue
        clusters.append(
            HolderCluster(
                root_funder=_root_funder(members, hop1, hop2),
                members=tuple(members),
            )
        )
    clusters.sort(key=lambda cluster: (-len(cluster.members), cluster.root_funder))
    return clusters


def _root_funder(
    members: list[str],
    hop1: dict[str, str],
    hop2: dict[str, str],
) -> str:
    """Корневой фандер компоненты: hop 2, если он есть, иначе hop 1.

    Если внутри компоненты несколько корней (union склеил их через общий
    промежуточный адрес), берём тот, на который выходит больше холдеров.
    При равенстве — меньший адрес, чтобы результат не прыгал между запусками.
    """
    counts: dict[str, int] = defaultdict(int)
    for member in members:
        direct = hop1.get(member)
        if direct is None:
            continue
        root = hop2.get(direct, direct)
        counts[root] += 1
    return min(counts, key=lambda address: (-counts[address], address))


def confidence_for(cluster_size: int, top_n_holders: int) -> Decimal:
    # TODO: временная эвристика. Откалибровать на реальных sybil-выборках,
    # прежде чем использовать score в алерте. Сейчас доля кластера в топе,
    # умноженная на 2 и обрезанная до 1: три холдера из двадцати дают 0.3.
    raw = (Decimal(cluster_size) / Decimal(top_n_holders)) * Decimal(2)
    return min(Decimal(1), raw)


class FunderGraphBuilder:
    """Снимает топ холдеров, пишет рёбра финансирования и значимые кластеры.

    ``build_graph_for_token`` делает flush, но не commit: вызывающий код
    решает, когда фиксировать транзакцию.

    ``getTokenLargestAccounts`` работает для любого SPL mint, не только для
    AMM. Исключение LP знает только программы Raydium, не bonding curve
    Pump.fun. Пока живой список холдеров кривой не проверен, pre-migration
    токены не должны попадать сюда: риск-скан смотрит
    ``funder_graph_status = skipped_pre_migration`` и не вызывает билдер.

    ``semaphore`` ограничивает число одновременных обходов. Внутри одного
    обхода запросы к Helius по-прежнему идут по очереди.
    """

    def __init__(
        self,
        session: AsyncSession,
        helius: FundingLookup,
        *,
        semaphore: asyncio.Semaphore | None = None,
    ) -> None:
        self._session = session
        self._helius = helius
        self._semaphore = semaphore

    async def root_for_wallet(self, address: str) -> str | None:
        """Funder root for one wallet, including the creator when they are not a top holder.

        Hop 1 is stored on a miss. Hop 2 is used when that edge is already stored
        or the lookup succeeds. A failed lookup of hop 1 leaves the root unknown.
        """
        try:
            direct = await self._load_cached_funding(address)
            if direct is None:
                direct = await self._helius.find_first_funding_tx(address)
                if direct is None:
                    logger.warning("creator_funder_unavailable", wallet=address, reason="no_funder")
                    return None
                await self._insert_edge(address, direct, hop_depth=1)
        except Exception as exc:
            failure = funder_lookup_failure(address, exc)
            if failure is None:
                raise
            logger.warning(
                "creator_funder_unavailable",
                wallet=address,
                reason=str(failure),
            )
            return None
        parent = await self._load_cached_funding(direct.funder)
        if parent is not None:
            return parent.funder
        try:
            parent_info = await self._helius.find_first_funding_tx(direct.funder)
        except Exception as exc:
            failure = funder_lookup_failure(direct.funder, exc)
            if failure is None:
                raise
            logger.warning(
                "creator_funder_unavailable",
                wallet=address,
                reason=str(failure),
            )
            return direct.funder
        if parent_info is None or parent_info.funder == direct.funder:
            return direct.funder
        await self._insert_edge(direct.funder, parent_info, hop_depth=2)
        return parent_info.funder

    async def build_graph_for_token(
        self,
        token_mint: str,
        top_n_holders: int = 20,
    ) -> list[FunderCluster]:
        if self._semaphore is None:
            return await self._build_graph_for_token(token_mint, top_n_holders)
        async with self._semaphore:
            return await self._build_graph_for_token(token_mint, top_n_holders)

    async def _build_graph_for_token(
        self,
        token_mint: str,
        top_n_holders: int,
    ) -> list[FunderCluster]:
        try:
            holders = await self._helius.get_top_holder_owners(token_mint, top_n_holders)
        except Exception as exc:
            failure = holder_list_failure(token_mint, exc)
            if failure is None:
                raise
            raise failure from exc
        unique_holders = list(dict.fromkeys(holders))
        funding_cache: dict[str, FundingInfo | None] = {}

        async def funding(address: str) -> FundingInfo | None:
            # Один и тот же адрес может быть и холдером, и фандером другого.
            # Первый SOL не меняется: успешный ответ лежит в wallet_funders
            # и в кэше этого прогона. Пустая история (None) кэшируется только
            # на время прогона — кошелёк ещё может получить первый SOL.
            if address not in funding_cache:
                cached = await self._load_cached_funding(address)
                if cached is not None:
                    funding_cache[address] = cached
                    logger.info("funding_lookup_cache_hit", wallet=address, funder=cached.funder)
                else:
                    try:
                        funding_cache[address] = await self._helius.find_first_funding_tx(address)
                    except Exception as exc:
                        failure = funder_lookup_failure(address, exc)
                        if failure is None:
                            raise
                        raise failure from exc
            return funding_cache[address]

        hop1: dict[str, FundingInfo] = {}
        for holder in unique_holders:
            info = await funding(holder)
            if info is not None and info.funder != holder:
                hop1[holder] = info

        hop2: dict[str, FundingInfo] = {}
        for direct in dict.fromkeys(info.funder for info in hop1.values()):
            info = await funding(direct)
            if info is not None and info.funder not in {direct}:
                hop2[direct] = info

        token = await self._token_for_mint(token_mint)
        await self._replace_edges(hop1, hop2)
        clusters = cluster_holders(
            unique_holders,
            {holder: info.funder for holder, info in hop1.items()},
            {funder: info.funder for funder, info in hop2.items()},
        )
        stored = await self._replace_clusters(token, clusters, top_n_holders)
        logger.info(
            "funder_graph_built",
            token_mint=token_mint,
            holders=len(unique_holders),
            edges=len(hop1) + len(hop2),
            clusters=len(stored),
        )
        return stored

    async def _token_for_mint(self, mint: str) -> Token:
        token = (
            await self._session.execute(select(Token).where(Token.mint == mint))
        ).scalar_one_or_none()
        if token is not None:
            return token
        token = Token(mint=mint, first_seen_at=datetime.now(timezone.utc))
        self._session.add(token)
        await self._session.flush()
        return token

    async def _replace_edges(
        self,
        hop1: dict[str, FundingInfo],
        hop2: dict[str, FundingInfo],
    ) -> None:
        for holder, info in hop1.items():
            await self._insert_edge(holder, info, hop_depth=1)
        for funder, info in hop2.items():
            await self._insert_edge(funder, info, hop_depth=2)

    async def _load_cached_funding(self, address: str) -> FundingInfo | None:
        row = await self._session.scalar(
            select(WalletFunder)
            .where(WalletFunder.wallet_address == address)
            .order_by(WalletFunder.hop_depth.asc())
            .limit(1)
        )
        if row is None:
            return None
        return FundingInfo(
            wallet_address=address,
            funder=row.funder_address,
            amount_sol=row.amount_sol,
            signature=row.funding_signature or "",
            funded_at=row.funded_at,
            slot=row.funding_slot,
        )

    async def _insert_edge(self, wallet_address: str, info: FundingInfo, hop_depth: int) -> None:
        statement = pg_insert(WalletFunder).values(
            wallet_address=wallet_address,
            funder_address=info.funder,
            amount_sol=info.amount_sol,
            hop_depth=hop_depth,
            funded_at=info.funded_at,
            funding_signature=info.signature or None,
            funding_slot=info.slot,
        )
        statement = statement.on_conflict_do_nothing(
            constraint="uq_wallet_funders_wallet_funder_hop",
        )
        await self._session.execute(statement)

    async def _replace_clusters(
        self,
        token: Token,
        clusters: list[HolderCluster],
        top_n_holders: int,
    ) -> list[FunderCluster]:
        # Повторный прогон по тому же mint заменяет прошлые кластеры,
        # а не копит дубли. Рёбра wallet_funders глобальные и остаются.
        await self._session.execute(delete(FunderCluster).where(FunderCluster.token_id == token.id))
        detected_at = datetime.now(timezone.utc)
        stored: list[FunderCluster] = []
        for cluster in clusters:
            row = FunderCluster(
                root_funder=cluster.root_funder,
                member_wallets=list(cluster.members),
                cluster_size=len(cluster.members),
                token_id=token.id,
                first_detected_at=detected_at,
                confidence=confidence_for(len(cluster.members), top_n_holders),
            )
            self._session.add(row)
            stored.append(row)
        await self._session.flush()
        return stored
