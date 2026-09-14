from pathlib import Path

import pytest

from memequant.protocols.decoder import ProtocolDecoder
from memequant.protocols.normalize import normalize_event
from memequant.utils import b58encode
from tests.helpers import (
    encode_event,
    envelope,
    load_idl,
    log_event,
    log_notification,
)

ROOT = Path(__file__).resolve().parents[2]
PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
AMM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"


def test_pump_trade_decode_and_normalize_without_float():
    idl = load_idl("pump.snapshot.json")
    mint = bytes([3]) * 32
    user = bytes([4]) * 32
    raw = encode_event(idl, "TradeEvent", {
        "mint": mint,
        "user": user,
        "token_amount": 9_223_372_036_854_775_000,
        "quote_amount": 1_234_567_890,
        "sol_amount": 1_111,
        "is_buy": True,
        "timestamp": 1_800_000_001,
        "shareholders": [],
    })
    dec = ProtocolDecoder("pump", ROOT / "src/memequant/idl/pump.snapshot.json")
    events, unknown = dec.decode_transaction(envelope(PUMP, log_event(PUMP, raw)))
    assert not unknown and len(events) == 1
    dataset, trade = normalize_event(events[0])
    assert dataset == "trades"
    assert trade.mint == b58encode(mint)
    assert trade.user == b58encode(user)
    assert trade.base_amount_raw == 9_223_372_036_854_775_000
    assert isinstance(trade.base_amount_raw, int)
    assert trade.quote_amount_raw == 1_234_567_890
    assert trade.side == "buy"


def test_full_transaction_and_log_notification_decode_shared_fields_identically():
    idl = load_idl("pump.snapshot.json")
    raw = encode_event(
        idl,
        "TradeEvent",
        {
            "token_amount": 99,
            "quote_amount": 42,
            "is_buy": False,
            "timestamp": 1_800_000_001,
            "shareholders": [],
        },
    )
    logs = log_event(PUMP, raw)
    dec = ProtocolDecoder("pump", ROOT / "src/memequant/idl/pump.snapshot.json")

    tx_events, tx_unknown = dec.decode_transaction(envelope(PUMP, logs))
    log_events, log_unknown = dec.decode_log_notification(log_notification(PUMP, logs))

    assert not tx_unknown and not log_unknown
    assert len(tx_events) == len(log_events) == 1
    tx_event = tx_events[0]
    log_event_decoded = log_events[0]
    for field in (
        "event_id",
        "protocol",
        "program_id",
        "event_type",
        "signature",
        "slot",
        "received_at",
        "log_index",
        "payload",
        "trailing_bytes",
    ):
        assert getattr(tx_event, field) == getattr(log_event_decoded, field)
    assert log_event_decoded.block_time is None
    assert log_event_decoded.transaction_index is None

    tx_dataset, tx_trade = normalize_event(tx_event)
    log_dataset, log_trade = normalize_event(log_event_decoded)
    assert tx_dataset == log_dataset == "trades"
    tx_shared = tx_trade.model_dump(exclude={"block_time"})
    log_shared = log_trade.model_dump(exclude={"block_time"})
    assert tx_shared == log_shared


@pytest.mark.parametrize(
    "event_type",
    ["CreateEvent", "TradeEvent", "CompleteEvent", "CompletePumpAmmMigrationEvent"],
)
def test_all_core_pump_events_normalize_from_log_notification(event_type: str):
    idl = load_idl("pump.snapshot.json")
    overrides = {"shareholders": []} if event_type == "TradeEvent" else {}
    raw = encode_event(idl, event_type, overrides)
    dec = ProtocolDecoder("pump", ROOT / "src/memequant/idl/pump.snapshot.json")
    events, unknown = dec.decode_log_notification(
        log_notification(PUMP, log_event(PUMP, raw))
    )
    assert not unknown
    assert len(events) == 1
    assert events[0].event_type == event_type
    assert normalize_event(events[0]) is not None


def test_schema_drift_extra_bytes_fails_closed():
    idl = load_idl("pump.snapshot.json")
    raw = encode_event(idl, "CompleteEvent") + b"new-field"
    dec = ProtocolDecoder("pump", ROOT / "src/memequant/idl/pump.snapshot.json")
    events, unknown = dec.decode_transaction(envelope(PUMP, log_event(PUMP, raw)))
    assert events == []
    assert len(unknown) == 1
    assert unknown[0].reason.startswith("schema-drift-trailing-bytes")


def test_pumpswap_buy_decode():
    idl = load_idl("pump_amm.snapshot.json")
    raw = encode_event(idl, "BuyEvent", {
        "base_amount_out": 50_000,
        "quote_amount_in": 20_000,
        "timestamp": 1_800_000_001,
    })
    dec = ProtocolDecoder("pumpswap", ROOT / "src/memequant/idl/pump_amm.snapshot.json")
    events, unknown = dec.decode_transaction(envelope(AMM, log_event(AMM, raw)))
    assert not unknown
    dataset, trade = normalize_event(events[0])
    assert dataset == "trades"
    assert trade.protocol == "pumpswap"
    assert trade.side == "buy"
    assert trade.base_amount_raw == 50_000
    assert trade.quote_amount_raw == 20_000


def test_pump_migration_links_mint_to_pool():
    idl = load_idl("pump.snapshot.json")
    mint = bytes([31]) * 32
    pool = bytes([32]) * 32
    raw = encode_event(idl, "CompletePumpAmmMigrationEvent", {
        "mint": mint,
        "pool": pool,
        "mint_amount": 123,
        "sol_amount": 456,
        "pool_migration_fee": 7,
    })
    dec = ProtocolDecoder("pump", ROOT / "src/memequant/idl/pump.snapshot.json")
    events, unknown = dec.decode_transaction(envelope(PUMP, log_event(PUMP, raw)))
    assert not unknown
    dataset, migration = normalize_event(events[0])
    assert dataset == "migrations"
    assert migration.mint == b58encode(mint)
    assert migration.pool == b58encode(pool)
    assert migration.quote_amount_raw == 456


def test_default_decoders_resolve_repository_root() -> None:
    from pathlib import Path

    from memequant.protocols.decoder import default_decoders

    repo_root = Path(__file__).resolve().parents[2]
    decoders = list(default_decoders(repo_root))
    assert [decoder.protocol for decoder in decoders] == ["pump", "pumpswap"]
    assert all(decoder.program_id for decoder in decoders)
