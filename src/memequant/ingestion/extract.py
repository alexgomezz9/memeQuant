from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

from memequant.models import RawTransactionEnvelope


def _signature(tx_entry: dict[str, Any]) -> str | None:
    transaction = tx_entry.get("transaction") or {}
    if isinstance(transaction, dict):
        signatures = transaction.get("signatures") or []
        if signatures:
            return signatures[0]
    return None



def envelopes_from_block_notification(
    message: dict[str, Any],
    *,
    program_id: str,
    provider: str,
    received_at: datetime,
) -> Iterable[RawTransactionEnvelope]:
    result = ((message.get("params") or {}).get("result") or {}).get("value") or {}
    slot = int(result.get("slot") or ((message.get("params") or {}).get("result") or {}).get("context", {}).get("slot", 0))
    block = result.get("block") or {}
    block_time = block.get("blockTime")
    # Per Solana's blockSubscribe contract, a mentionsAccountOrProgram-filtered
    # notification already contains only matching transactions. Do not re-filter by
    # successful invocation logs: a failed transaction may mention the program without
    # reaching its invocation and is still valuable RAW audit data.
    for tx_index, tx_entry in enumerate(block.get("transactions") or []):
        sig = _signature(tx_entry)
        if not sig:
            continue
        # Shape this like getTransaction so live and reconciled paths share one decoder.
        rpc_tx = {
            "slot": slot,
            "blockTime": block_time,
            "transaction": tx_entry.get("transaction"),
            "meta": tx_entry.get("meta"),
            "version": tx_entry.get("version"),
        }
        yield RawTransactionEnvelope(
            source_provider=provider,
            source_mode="block",
            source_program=program_id,
            slot=slot,
            block_time=block_time,
            received_at=received_at,
            signature=sig,
            transaction_index=tx_index,
            rpc_transaction=rpc_tx,
        )


def envelope_from_get_transaction(
    result: dict[str, Any],
    *,
    signature: str,
    program_id: str,
    provider: str,
    received_at: datetime,
    source_mode: str,
) -> RawTransactionEnvelope:
    return RawTransactionEnvelope(
        source_provider=provider,
        source_mode=source_mode,
        source_program=program_id,
        slot=int(result["slot"]),
        block_time=result.get("blockTime"),
        received_at=received_at,
        signature=signature,
        transaction_index=None,
        rpc_transaction=result,
    )
