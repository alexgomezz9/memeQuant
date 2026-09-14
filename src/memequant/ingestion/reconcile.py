from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from memequant.ingestion.extract import envelope_from_get_transaction
from memequant.rpc import SolanaHttpRpc
from memequant.storage.state import StateStore
from memequant.utils import utc_now

log = logging.getLogger(__name__)


class ReconciliationError(RuntimeError):
    """Recovery cannot continue without risking an unreported data gap."""


class ReconciliationLimitExceeded(ReconciliationError):
    """Raised rather than silently skipping an unbounded historical gap."""


class ReconciliationTransactionUnavailable(ReconciliationError):
    """Raised when a missing signature cannot be fetched safely during recovery."""


class Reconciler:
    """Recover transactions missed while the websocket was disconnected.

    A filtered block stream legitimately skips slots with no matching transaction, so
    slot-number gaps are *not* a loss signal. Recovery walks the program's signatures
    from the current chain tip back to the last per-program checkpoint.
    """

    def __init__(
        self,
        rpc: SolanaHttpRpc,
        state: StateStore,
        provider: str,
        commitment: str,
        max_signatures: int = 2000,
        metric_sink: Callable[[str, int], None] | None = None,
    ):
        self.rpc = rpc
        self.state = state
        self.provider = provider
        self.commitment = commitment
        self.max_signatures = max_signatures
        self._metric_sink = metric_sink

    def _count(self, key: str, amount: int = 1) -> None:
        if self._metric_sink is None:
            self.state.increment(key, amount)
        else:
            self._metric_sink(key, amount)

    async def bootstrap_if_needed(self, program_id: str) -> bool:
        """Set a start boundary on a brand-new collector without historical backfill.

        The websocket is subscribed immediately afterwards and reconciliation is run
        *after* subscriptions are active, closing the bootstrap->subscribe race window.
        """
        if self.state.get_checkpoint(program_id) is not None:
            return False
        page = await self.rpc.get_signatures_for_address(
            program_id, commitment=self.commitment, limit=1
        )
        if not page:
            return False
        item = page[0]
        self.state.set_checkpoint(
            program_id,
            item["signature"],
            int(item["slot"]),
            utc_now().isoformat(),
        )
        return True

    async def missing_signatures(self, program_id: str) -> list[dict]:
        checkpoint = self.state.get_checkpoint(program_id)
        until = checkpoint["last_signature"] if checkpoint else None
        if not until or self.max_signatures == 0:
            return []

        found: list[dict] = []
        before: str | None = None
        exhausted = False
        while len(found) < self.max_signatures:
            requested = min(1000, self.max_signatures - len(found))
            page = await self.rpc.get_signatures_for_address(
                program_id,
                commitment=self.commitment,
                until=until,
                before=before,
                limit=requested,
            )
            if not page:
                exhausted = True
                break
            found.extend(page)
            if len(page) < requested:
                exhausted = True
                break
            before = page[-1]["signature"]

        # Never advance over an unprocessed middle of history. If the configured cap
        # is insufficient, fail closed and let the operator increase it.
        if not exhausted and found:
            probe = await self.rpc.get_signatures_for_address(
                program_id,
                commitment=self.commitment,
                until=until,
                before=found[-1]["signature"],
                limit=1,
            )
            if probe:
                raise ReconciliationLimitExceeded(
                    f"more than {self.max_signatures} signatures are missing for {program_id}; "
                    "collector halted without advancing the checkpoint; inspect the gap and "
                    "run a deliberate bounded recovery before resuming"
                )

        # Failed signatures cannot have committed economic effects and logsSubscribe gives
        # us the same error status directly. Do not spend getTransaction credits on them
        # during recovery. Keep a counter so the filtering remains observable.
        successful = [item for item in found if item.get("err") is None]
        skipped = len(found) - len(successful)
        if skipped:
            self.state.increment("reconcile_failed_signatures_skipped", skipped)

        # RPC returns newest -> oldest. Replay oldest -> newest for deterministic state.
        return list(reversed(successful))

    async def _get_transaction_or_fail(self, signature: str) -> dict:
        # A signature may become visible slightly before the full transaction is available
        # from a provider. Never skip it and continue to newer signatures: doing so could
        # advance the checkpoint across a permanent hole in RAW.
        delay = 0.15
        for attempt in range(5):
            result = await self.rpc.get_transaction(signature, commitment=self.commitment)
            if result is not None:
                return result
            if attempt < 4:
                self._count("getTransaction_retries")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 1.2)
        raise ReconciliationTransactionUnavailable(
            f"signature {signature} was listed by getSignaturesForAddress but "
            "getTransaction remained unavailable; recovery stopped before advancing "
            "the checkpoint"
        )

    async def reconcile(self, program_id: str, process: Callable[[object], bool]) -> int:
        signatures = await self.missing_signatures(program_id)
        observed = 0
        for item in signatures:
            signature = item["signature"]
            # Even when the same transaction was already seen through the other program,
            # process it so IngestionEngine can record this program observation/checkpoint.
            if self.state.observation_seen(signature, program_id):
                continue
            result = await self._get_transaction_or_fail(signature)
            env = envelope_from_get_transaction(
                result,
                signature=signature,
                program_id=program_id,
                provider=self.provider,
                received_at=utc_now(),
                source_mode="reconcile",
            )
            process(env)
            observed += 1
            await asyncio.sleep(0)
        return observed
