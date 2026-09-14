from __future__ import annotations

from memequant.models import (
    CompletionRecord,
    DecodedEvent,
    MigrationRecord,
    PoolRecord,
    TokenRecord,
    TradeRecord,
)


def _i(payload: dict, key: str, default: int | None = None) -> int | None:
    value = payload.get(key, default)
    return None if value is None else int(value)


def normalize_event(event: DecodedEvent):
    p = event.payload

    if event.protocol == "pump" and event.event_type == "CreateEvent":
        return "tokens", TokenRecord(
            event_id=event.event_id,
            mint=p["mint"],
            creator=p["creator"],
            user=p["user"],
            name=p["name"],
            symbol=p["symbol"],
            uri=p["uri"],
            bonding_curve=p["bonding_curve"],
            token_program=p["token_program"],
            quote_mint=p.get("quote_mint"),
            event_timestamp=int(p["timestamp"]),
            slot=event.slot,
            block_time=event.block_time,
            received_at=event.received_at,
            signature=event.signature,
            virtual_token_reserves=int(p["virtual_token_reserves"]),
            virtual_sol_reserves=_i(p, "virtual_sol_reserves"),
            virtual_quote_reserves=_i(p, "virtual_quote_reserves"),
            real_token_reserves=int(p["real_token_reserves"]),
            token_total_supply=int(p["token_total_supply"]),
            is_mayhem_mode=bool(p.get("is_mayhem_mode", False)),
            is_cashback_enabled=bool(p.get("is_cashback_enabled", False)),
        )

    if event.protocol == "pump" and event.event_type == "TradeEvent":
        quote_amount = p.get("quote_amount")
        if quote_amount in (None, 0):
            quote_amount = p.get("sol_amount", 0)
        return "trades", TradeRecord(
            event_id=event.event_id,
            protocol="pump",
            side="buy" if p["is_buy"] else "sell",
            mint=p["mint"],
            user=p["user"],
            quote_mint=p.get("quote_mint"),
            base_amount_raw=int(p["token_amount"]),
            quote_amount_raw=int(quote_amount),
            slot=event.slot,
            block_time=event.block_time,
            event_timestamp=int(p["timestamp"]),
            received_at=event.received_at,
            signature=event.signature,
            virtual_base_reserves=_i(p, "virtual_token_reserves"),
            virtual_quote_reserves=_i(p, "virtual_quote_reserves", _i(p, "virtual_sol_reserves")),
            real_base_reserves=_i(p, "real_token_reserves"),
            real_quote_reserves=_i(p, "real_quote_reserves", _i(p, "real_sol_reserves")),
            protocol_fee_raw=_i(p, "fee"),
            creator_fee_raw=_i(p, "creator_fee"),
            cashback_raw=_i(p, "cashback"),
            buyback_fee_raw=_i(p, "buyback_fee"),
            ix_name=p.get("ix_name"),
        )

    if event.protocol == "pump" and event.event_type == "CompleteEvent":
        return "completions", CompletionRecord(
            event_id=event.event_id,
            mint=p["mint"],
            user=p["user"],
            bonding_curve=p["bonding_curve"],
            quote_mint=p.get("quote_mint"),
            event_timestamp=int(p["timestamp"]),
            slot=event.slot,
            block_time=event.block_time,
            received_at=event.received_at,
            signature=event.signature,
        )

    if event.protocol == "pump" and event.event_type == "CompletePumpAmmMigrationEvent":
        return "migrations", MigrationRecord(
            event_id=event.event_id,
            mint=p["mint"],
            user=p["user"],
            bonding_curve=p["bonding_curve"],
            pool=p["pool"],
            quote_mint=p["quote_mint"],
            mint_amount_raw=int(p["mint_amount"]),
            quote_amount_raw=int(p["sol_amount"]),
            migration_fee_raw=int(p["pool_migration_fee"]),
            event_timestamp=int(p["timestamp"]),
            slot=event.slot,
            block_time=event.block_time,
            received_at=event.received_at,
            signature=event.signature,
        )

    if event.protocol == "pumpswap" and event.event_type == "CreatePoolEvent":
        return "pools", PoolRecord(
            event_id=event.event_id,
            pool=p["pool"],
            creator=p["creator"],
            base_mint=p["base_mint"],
            quote_mint=p["quote_mint"],
            base_mint_decimals=int(p["base_mint_decimals"]),
            quote_mint_decimals=int(p["quote_mint_decimals"]),
            event_timestamp=int(p["timestamp"]),
            slot=event.slot,
            block_time=event.block_time,
            received_at=event.received_at,
            signature=event.signature,
            base_amount_in=int(p["base_amount_in"]),
            quote_amount_in=int(p["quote_amount_in"]),
            is_mayhem_mode=bool(p.get("is_mayhem_mode", False)),
        )

    if event.protocol == "pumpswap" and event.event_type in {"BuyEvent", "SellEvent"}:
        is_buy = event.event_type == "BuyEvent"
        return "trades", TradeRecord(
            event_id=event.event_id,
            protocol="pumpswap",
            side="buy" if is_buy else "sell",
            pool=p["pool"],
            user=p["user"],
            base_amount_raw=int(p["base_amount_out"] if is_buy else p["base_amount_in"]),
            quote_amount_raw=int(p["quote_amount_in"] if is_buy else p["quote_amount_out"]),
            slot=event.slot,
            block_time=event.block_time,
            event_timestamp=int(p["timestamp"]),
            received_at=event.received_at,
            signature=event.signature,
            virtual_quote_reserves=_i(p, "virtual_quote_reserves"),
            real_base_reserves=_i(p, "pool_base_token_reserves"),
            real_quote_reserves=_i(p, "pool_quote_token_reserves"),
            protocol_fee_raw=_i(p, "protocol_fee"),
            creator_fee_raw=_i(p, "coin_creator_fee"),
            lp_fee_raw=_i(p, "lp_fee"),
            cashback_raw=_i(p, "cashback"),
            buyback_fee_raw=_i(p, "buyback_fee"),
            ix_name=p.get("ix_name"),
        )

    return None
