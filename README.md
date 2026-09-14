# MemeQuant Data Engine v0

A read-only, point-in-time Solana/Pump.fun data collector designed for **research first**. It does not trade, hold keys, or claim an edge. Its job is to preserve enough trustworthy raw data that later ML/backtests can be audited and rebuilt.

> Research hypothesis: assume there is **no tradable edge after fees, slippage, latency and selection effects** until out-of-sample evidence says otherwise.

## Scope of v0

Implemented:

- Pump.fun live transaction collection through standard Solana JSON-RPC/WebSocket.
- Preferred `blockSubscribe` mode with automatic fallback to `logsSubscribe + getTransaction` when a provider does not support block subscriptions.
- Per-program recovery with `getSignaturesForAddress` + `getTransaction` after disconnects.
- Race-safe startup: establish a chain-tip boundary, subscribe, then reconcile while websocket messages queue.
- Authoritative append-only RAW archive (`JSONL.GZ`).
- Current Pump event decoding for `CreateEvent`, `TradeEvent`, `CompleteEvent`, and `CompletePumpAmmMigrationEvent`.
- Current PumpSwap decoding for `CreatePoolEvent`, `BuyEvent`, and `SellEvent`.
- Typed normalized datasets for tokens, trades, completions, migrations and pools.
- Integer-only on-chain amounts; no financial `float` conversion.
- Transaction and event idempotency with SQLite state/checkpoints.
- Failed Solana transactions retained in RAW but excluded from economic event tables.
- Strict schema-drift behavior: unknown discriminators, short payloads, or extra trailing event bytes are quarantined rather than silently normalized.
- Deterministic rebuild of derived datasets from RAW.
- Tests for Borsh decoding, nested program-log attribution, storage, dedupe, cross-program observations, failed transactions, reconciliation limits, RPC configuration and rebuilds.

Not implemented yet (deliberately):

- token scoring / ML;
- labels or backtesting;
- smart-wallet scoring;
- wallet funding graph;
- off-chain metadata/image/social snapshotting;
- order construction or live execution;
- private keys;
- historical IDL-version catalog for decoding old Pump event layouts;
- finalized-fork correction layer.

Those belong after the live data path is measured and validated against real mainnet transactions.

## Why RAW is the source of truth

`data/raw` is the authoritative ledger. `data/normalized` is only a convenience derived cache. If the process crashes during a buffered Parquet write or the protocol schema changes, research data must be regenerated from RAW with `memequant-replay`.

RAW rows are flushed before a transaction checkpoint advances. Duplicate RAW rows are allowed after crash/reconciliation; deterministic rebuild deduplicates them by signature/event ID. This design prefers recoverable duplication over silent loss.

## Programs

- Pump: `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`
- PumpSwap: `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA`

The bundled event schemas were transcribed from the official `pump-fun/pump-public-docs` IDLs on **2026-09-10**. Before a long-running deployment, run `python scripts/update_idl.py`, review the diff against the bundled schemas, and run the tests. Never auto-promote a changed IDL in production.

Official sources:

- https://github.com/pump-fun/pump-public-docs
- https://raw.githubusercontent.com/pump-fun/pump-public-docs/main/idl/pump.json
- https://raw.githubusercontent.com/pump-fun/pump-public-docs/main/idl/pump_amm.json
- https://solana.com/docs/rpc/websocket/blocksubscribe
- https://solana.com/docs/rpc/websocket/logssubscribe
- https://solana.com/docs/rpc/http/getsignaturesforaddress
- https://solana.com/docs/rpc/http/gettransaction

## Quick start

Requires Python 3.12+.

```bash
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# macOS/Linux: source .venv/bin/activate
python -m pip install -U pip
pip install -e ".[dev]"
cp .env.example .env  # Windows: copy .env.example .env
```

Put your own RPC URLs in `.env`. Do **not** paste keys into source code or commit `.env`.

A Helius-style configuration looks like:

```dotenv
SOLANA_HTTP_RPC_URL=https://mainnet.helius-rpc.com/?api-key=YOUR_KEY
SOLANA_WS_RPC_URL=wss://mainnet.helius-rpc.com/?api-key=YOUR_KEY
MEMEQUANT_PROVIDER_NAME=helius
```

Start cheaply with Pump only:

```dotenv
MEMEQUANT_PROTOCOLS=pump
```

Collect:

```bash
memequant-collector
```

or with Docker:

```bash
docker compose up --build collector
```

Inspect persistent state/file sizes:

```bash
memequant-stats
# or
python scripts/stats.py
```

Validate the local installation:

```bash
memequant-doctor
```

After adding your own RPC credentials, validate HTTP + WebSocket capability without collecting:

```bash
memequant-doctor --network
```

Run tests:

```bash
pytest
```

See [`docs/VALIDATION.md`](docs/VALIDATION.md) for the exact boundary between what has
been tested offline and what still requires your own live RPC endpoint.

## Data layout

```text
data/
├── raw/
│   └── date=YYYY-MM-DD/hour=HH/transactions.jsonl.gz
├── normalized/
│   ├── events/date=YYYY-MM-DD/part-*.parquet
│   ├── tokens/date=YYYY-MM-DD/part-*.parquet
│   ├── trades/date=YYYY-MM-DD/part-*.parquet
│   ├── completions/date=YYYY-MM-DD/part-*.parquet
│   ├── migrations/date=YYYY-MM-DD/part-*.parquet
│   ├── pools/date=YYYY-MM-DD/part-*.parquet
│   └── unknown_program_data/date=YYYY-MM-DD/part-*.parquet
└── state/collector.sqlite3
```

If PyArrow is unavailable, `NormalizedStore` falls back to `.jsonl.gz` so collection remains testable. Normal installation includes PyArrow and writes Parquet.

## Rebuild research datasets

Do not treat the live normalized cache as canonical after a crash, decoder upgrade, or IDL change. Rebuild from RAW:

```bash
memequant-replay --output data/rebuilt_normalized
```

The rebuild writes to a temporary directory and only swaps the completed result into place after success. RAW duplicates are deduplicated.

## DuckDB

After Parquet data exists:

```bash
python scripts/query_duckdb.py
```

Example:

```sql
SELECT
  protocol,
  side,
  count(*) AS n,
  sum(quote_amount_raw) AS quote_raw
FROM read_parquet('data/rebuilt_normalized/trades/date=*/*.parquet', union_by_name=true)
GROUP BY ALL;
```

Do not convert raw integer units to floating-point money inside the ingestion layer. Unit conversion belongs in a research/view layer with explicit decimals and quote mint.

PumpSwap `BuyEvent`/`SellEvent` identify the pool but do not carry the base mint directly.
Resolve the mint point-in-time through `pools`/`migrations`; the ingestion layer deliberately
does not guess that relationship.

## Subscription modes

`MEMEQUANT_SUBSCRIPTION_MODE=auto` first attempts `blockSubscribe`. Solana documents `blockSubscribe` as unstable and validators/providers may disable it. If the subscription RPC rejects it, the collector falls back to `logsSubscribe`, then fetches each matching transaction over HTTP.

A filtered `blockSubscribe` **does not emit slots containing no matching transaction**. Non-consecutive observed slot numbers are therefore normal and must not be called data gaps. Recovery uses program-address signatures instead.

Start with:

```dotenv
MEMEQUANT_PROTOCOLS=pump
```

Capturing the entire PumpSwap program can be far more bandwidth-intensive. Enable it only after measuring provider credits/bandwidth:

```dotenv
MEMEQUANT_PROTOCOLS=pump,pumpswap
```

Pump-only mode still decodes PumpSwap events when Pump invokes PumpSwap inside the same captured transaction (e.g. migration), because decoding is transaction-wide.

## Recovery invariants

1. The latest chain signature becomes the boundary on the first run; v0 does not unexpectedly backfill old history.
2. Websocket subscriptions are established before reconnect reconciliation.
3. While HTTP reconciliation runs, websocket notifications queue and are later deduplicated.
4. A transaction can be observed through both Pump and PumpSwap. It is decoded once, but each program gets its own observation/checkpoint.
5. If the number of missed signatures exceeds `MEMEQUANT_MAX_RECONCILE_SIGNATURES`, recovery **fails closed** instead of jumping the checkpoint over unknown history.
6. Checkpoints cannot regress to a lower slot.

See `docs/RECOVERY.md` for caveats.

## Transaction versions

As of 2026-09-10, Solana mainnet v1 transactions are not yet active and the stable RPC docs still use `maxSupportedTransactionVersion: 0`. The project exposes:

```dotenv
MEMEQUANT_MAX_SUPPORTED_TRANSACTION_VERSION=0
```

When v1 activates, update this to `1` only after verifying provider support. Solana warns that clients capped at v0 can fail on a v1 transaction, including `blockSubscribe` returning `block: null` behavior; the collector treats a null block as an error rather than silently moving on.

## Research integrity

Every later feature must obey:

```text
feature.available_at <= decision_time
```

Wallet reputation is especially dangerous. A wallet's score on day `t` may use only outcomes that were already resolved by `t`; calculating today's “best wallets” and attaching that score to historical trades is look-ahead leakage.

Similarly, token metadata fetched after launch must not be used as if it were known at launch. v0 stores the creation-time URI but intentionally does not yet fetch arbitrary attacker-controlled URLs.

Read `docs/DATA_CONTRACT.md` and `docs/RESEARCH_CONTRACT.md` before building labels/models.

## Security

This repository is read-only by design. It needs no wallet and no private key. Do not add a trading key to `.env` during the research phases. If live execution is ever justified by paper-trading evidence, it should live behind a separate execution package/service with a dedicated low-balance wallet, limits and kill switch.

## Known limitations

See `docs/LIMITATIONS.md`. The most important ones are: live mainnet behavior still requires validation with your RPC account; bundled IDLs describe the current event layouts rather than every historical Pump version; off-chain metadata is not snapshotted yet; and v0 does not reconcile confirmed observations against later Solana finality/forks.

## Security notes

- Live collection is **read-only**. MemeQuant v0 does not load private keys, connect a wallet, sign transactions, or submit trades.
- RPC endpoints are required to use TLS (`https://` and `wss://`). Plaintext `http://` / `ws://` endpoints are rejected.
- Keep RPC credentials only in `.env`; `.env` is gitignored. Never paste a real RPC key into issues, commits, screenshots, or chat logs.
- `memequant-doctor` redacts credentials derived from configured RPC URLs before printing caught network errors.
- The normal collector connects only to the RPC URLs that **you configure**. The bundled Helius example uses Helius' official mainnet HTTPS/WSS endpoints.
- `scripts/update_idl.py` is an optional, manual maintenance script that downloads IDLs from Pump.fun's public GitHub repository. It is not run by the collector and downloaded changes must be reviewed before promotion.
- This project deliberately contains no live execution code in v0. If execution is added later, it should use a dedicated low-balance bot wallet and a separate security review.
