from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RawTransactionEnvelope(StrictModel):
    source_provider: str
    source_mode: Literal["block", "logs", "reconcile", "replay"]
    source_program: str
    slot: int
    block_time: int | None = None
    received_at: datetime
    signature: str
    transaction_index: int | None = None
    rpc_transaction: dict[str, Any]


class DecodedEvent(StrictModel):
    event_id: str
    protocol: Literal["pump", "pumpswap"]
    program_id: str
    event_type: str
    signature: str
    slot: int
    block_time: int | None
    received_at: datetime
    transaction_index: int | None
    log_index: int
    payload: dict[str, Any]
    trailing_bytes: int = 0


class UnknownProgramData(StrictModel):
    event_id: str
    protocol: Literal["pump", "pumpswap"]
    program_id: str
    signature: str
    slot: int
    block_time: int | None
    received_at: datetime
    transaction_index: int | None
    log_index: int
    discriminator_hex: str
    payload_size: int
    reason: str


class TokenRecord(StrictModel):
    event_id: str
    mint: str
    creator: str
    user: str
    name: str
    symbol: str
    uri: str
    bonding_curve: str
    token_program: str
    quote_mint: str | None = None
    event_timestamp: int
    slot: int
    block_time: int | None
    received_at: datetime
    signature: str
    virtual_token_reserves: int
    virtual_sol_reserves: int | None = None
    virtual_quote_reserves: int | None = None
    real_token_reserves: int
    token_total_supply: int
    is_mayhem_mode: bool = False
    is_cashback_enabled: bool = False


class TradeRecord(StrictModel):
    event_id: str
    protocol: Literal["pump", "pumpswap"]
    side: Literal["buy", "sell"]
    mint: str | None = None
    pool: str | None = None
    user: str
    quote_mint: str | None = None
    base_amount_raw: int
    quote_amount_raw: int
    slot: int
    block_time: int | None
    event_timestamp: int
    received_at: datetime
    signature: str
    virtual_base_reserves: int | None = None
    virtual_quote_reserves: int | None = None
    real_base_reserves: int | None = None
    real_quote_reserves: int | None = None
    protocol_fee_raw: int | None = None
    creator_fee_raw: int | None = None
    lp_fee_raw: int | None = None
    cashback_raw: int | None = None
    buyback_fee_raw: int | None = None
    ix_name: str | None = None


class CompletionRecord(StrictModel):
    event_id: str
    mint: str
    user: str
    bonding_curve: str
    quote_mint: str | None = None
    event_timestamp: int
    slot: int
    block_time: int | None
    received_at: datetime
    signature: str


class MigrationRecord(StrictModel):
    event_id: str
    mint: str
    user: str
    bonding_curve: str
    pool: str
    quote_mint: str
    mint_amount_raw: int
    quote_amount_raw: int
    migration_fee_raw: int
    event_timestamp: int
    slot: int
    block_time: int | None
    received_at: datetime
    signature: str


class PoolRecord(StrictModel):
    event_id: str
    pool: str
    creator: str
    base_mint: str
    quote_mint: str
    base_mint_decimals: int
    quote_mint_decimals: int
    event_timestamp: int
    slot: int
    block_time: int | None
    received_at: datetime
    signature: str
    base_amount_in: int
    quote_amount_in: int
    is_mayhem_mode: bool = False


class CollectorMetrics(StrictModel):
    started_at: datetime
    websocket_connections: int = 0
    reconnects: int = 0
    notifications: int = 0
    transactions_seen: int = 0
    duplicate_transactions: int = 0
    failed_transactions: int = 0
    events_decoded: int = 0
    unknown_program_data: int = 0
    tokens_created: int = 0
    trades: int = 0
    completions: int = 0
    migrations: int = 0
    pools_created: int = 0
    reconciled_transactions: int = 0
    rpc_errors: int = 0
