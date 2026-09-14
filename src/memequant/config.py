from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from memequant.security import validate_secure_rpc_url


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    solana_http_rpc_url: str | None = Field(default=None, alias="SOLANA_HTTP_RPC_URL")
    solana_ws_rpc_url: str | None = Field(default=None, alias="SOLANA_WS_RPC_URL")

    data_dir: Path = Field(default=Path("data"), alias="MEMEQUANT_DATA_DIR")
    provider_name: str = Field(default="solana-rpc", alias="MEMEQUANT_PROVIDER_NAME")
    commitment: Literal["confirmed", "finalized"] = Field(
        default="confirmed", alias="MEMEQUANT_COMMITMENT"
    )
    subscription_mode: Literal["auto", "block", "logs"] = Field(
        default="auto", alias="MEMEQUANT_SUBSCRIPTION_MODE"
    )
    protocols: str = Field(default="pump", alias="MEMEQUANT_PROTOCOLS")

    reconcile_on_start: bool = Field(default=True, alias="MEMEQUANT_RECONCILE_ON_START")
    reconcile_on_reconnect: bool = Field(
        default=True, alias="MEMEQUANT_RECONCILE_ON_RECONNECT"
    )
    max_reconcile_signatures: int = Field(
        default=2000, ge=0, le=100_000, alias="MEMEQUANT_MAX_RECONCILE_SIGNATURES"
    )
    http_concurrency: int = Field(default=8, ge=1, le=64, alias="MEMEQUANT_HTTP_CONCURRENCY")
    http_rps_limit: float = Field(
        default=6.0, ge=0.5, le=500.0, alias="MEMEQUANT_HTTP_RPS_LIMIT"
    )
    ws_queue_maxsize: int = Field(
        default=5_000, ge=1, le=100_000, alias="MEMEQUANT_WS_QUEUE_MAXSIZE"
    )
    parquet_batch_size: int = Field(
        default=500, ge=1, le=100_000, alias="MEMEQUANT_PARQUET_BATCH_SIZE"
    )
    raw_fsync_every: int = Field(default=100, ge=0, alias="MEMEQUANT_RAW_FSYNC_EVERY")
    max_supported_transaction_version: int = Field(
        default=1, ge=0, le=1, alias="MEMEQUANT_MAX_SUPPORTED_TRANSACTION_VERSION"
    )
    reconnect_min_seconds: float = Field(default=1.0, ge=0.1)
    reconnect_max_seconds: float = Field(default=30.0, ge=1.0)
    log_level: str = Field(default="INFO", alias="MEMEQUANT_LOG_LEVEL")

    @field_validator("solana_http_rpc_url")
    @classmethod
    def validate_http_rpc_url(cls, value: str | None) -> str | None:
        if value is None:
            return value
        return validate_secure_rpc_url(value, websocket=False)

    @field_validator("solana_ws_rpc_url")
    @classmethod
    def validate_ws_rpc_url(cls, value: str | None) -> str | None:
        if value is None:
            return value
        return validate_secure_rpc_url(value, websocket=True)

    @model_validator(mode="after")
    def validate_reconnect(self) -> "Settings":
        if self.reconnect_max_seconds < self.reconnect_min_seconds:
            raise ValueError("reconnect_max_seconds must be >= reconnect_min_seconds")
        return self

    @property
    def enabled_protocols(self) -> tuple[str, ...]:
        allowed = {"pump", "pumpswap"}
        values = tuple(x.strip().lower() for x in self.protocols.split(",") if x.strip())
        unknown = set(values) - allowed
        if not values or unknown:
            raise ValueError(
                f"MEMEQUANT_PROTOCOLS must contain pump and/or pumpswap; invalid={sorted(unknown)}"
            )
        return values

    @property
    def state_db_path(self) -> Path:
        return self.data_dir / "state" / "collector.sqlite3"

    def require_live_urls(self) -> None:
        if not self.solana_http_rpc_url or not self.solana_ws_rpc_url:
            raise RuntimeError(
                "Live collection requires SOLANA_HTTP_RPC_URL and SOLANA_WS_RPC_URL. "
                "Copy .env.example to .env and insert your own RPC credentials locally."
            )
