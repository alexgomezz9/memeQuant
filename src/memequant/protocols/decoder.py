from __future__ import annotations

from pathlib import Path
from typing import Iterable

from memequant.models import DecodedEvent, RawTransactionEnvelope, UnknownProgramData
from memequant.protocols.anchor import BorshDecodeError, IdlEventDecoder, iter_program_data
from memequant.utils import stable_event_id


class ProtocolDecoder:
    def __init__(self, protocol: str, idl_path: Path):
        if protocol not in {"pump", "pumpswap"}:
            raise ValueError(f"unsupported protocol {protocol}")
        self.protocol = protocol
        self.idl_decoder = IdlEventDecoder(idl_path)
        self.program_id = self.idl_decoder.program_id

    def decode_transaction(
        self, envelope: RawTransactionEnvelope
    ) -> tuple[list[DecodedEvent], list[UnknownProgramData]]:
        tx_result = envelope.rpc_transaction
        meta = tx_result.get("meta") or {}
        logs = meta.get("logMessages") or meta.get("log_messages") or []
        events: list[DecodedEvent] = []
        unknown: list[UnknownProgramData] = []

        for log_index, program_id, raw in iter_program_data(logs):
            if program_id != self.program_id:
                continue
            event_id_base = stable_event_id(
                envelope.signature, program_id, log_index, "program-data"
            )
            try:
                decoded = self.idl_decoder.decode(raw)
            except BorshDecodeError as exc:
                unknown.append(
                    UnknownProgramData(
                        event_id=event_id_base,
                        protocol=self.protocol,
                        program_id=program_id,
                        signature=envelope.signature,
                        slot=envelope.slot,
                        block_time=envelope.block_time,
                        received_at=envelope.received_at,
                        transaction_index=envelope.transaction_index,
                        log_index=log_index,
                        discriminator_hex=raw[:8].hex(),
                        payload_size=len(raw),
                        reason=f"decode-error: {exc}",
                    )
                )
                continue

            if decoded is None:
                unknown.append(
                    UnknownProgramData(
                        event_id=event_id_base,
                        protocol=self.protocol,
                        program_id=program_id,
                        signature=envelope.signature,
                        slot=envelope.slot,
                        block_time=envelope.block_time,
                        received_at=envelope.received_at,
                        transaction_index=envelope.transaction_index,
                        log_index=log_index,
                        discriminator_hex=raw[:8].hex(),
                        payload_size=len(raw),
                        reason="unknown-discriminator",
                    )
                )
                continue

            if decoded.trailing_bytes:
                unknown.append(
                    UnknownProgramData(
                        event_id=event_id_base,
                        protocol=self.protocol,
                        program_id=program_id,
                        signature=envelope.signature,
                        slot=envelope.slot,
                        block_time=envelope.block_time,
                        received_at=envelope.received_at,
                        transaction_index=envelope.transaction_index,
                        log_index=log_index,
                        discriminator_hex=raw[:8].hex(),
                        payload_size=len(raw),
                        reason=f"schema-drift-trailing-bytes:{decoded.trailing_bytes}",
                    )
                )
                continue

            event_id = stable_event_id(
                envelope.signature, program_id, log_index, decoded.event_type
            )
            events.append(
                DecodedEvent(
                    event_id=event_id,
                    protocol=self.protocol,
                    program_id=program_id,
                    event_type=decoded.event_type,
                    signature=envelope.signature,
                    slot=envelope.slot,
                    block_time=envelope.block_time,
                    received_at=envelope.received_at,
                    transaction_index=envelope.transaction_index,
                    log_index=log_index,
                    payload=decoded.payload,
                    trailing_bytes=decoded.trailing_bytes,
                )
            )

        return events, unknown


def default_decoders(repo_root: Path | None = None) -> Iterable[ProtocolDecoder]:
    if repo_root is None:
        idl_dir = Path(__file__).resolve().parents[1] / "idl"
    else:
        repo_root = repo_root.resolve()
        candidates = (repo_root / "src" / "memequant" / "idl", repo_root / "idl")
        idl_dir = next((candidate for candidate in candidates if candidate.is_dir()), candidates[0])
    return (
        ProtocolDecoder("pump", idl_dir / "pump.snapshot.json"),
        ProtocolDecoder("pumpswap", idl_dir / "pump_amm.snapshot.json"),
    )
