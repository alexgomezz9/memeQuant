#!/usr/bin/env python3
from pathlib import Path

try:
    import duckdb
except ImportError as exc:
    raise SystemExit("Install project dependencies first: pip install -e .") from exc

root = Path("data/normalized")
for dataset in ("tokens", "trades", "completions", "migrations", "pools", "unknown_program_data"):
    files = list((root / dataset).glob("date=*/*.parquet"))
    if not files:
        continue
    glob = str(root / dataset / "date=*" / "*.parquet").replace("'", "''")
    n = duckdb.sql(f"SELECT count(*) FROM read_parquet('{glob}', union_by_name=true)").fetchone()[0]
    print(f"{dataset:24s} {n:12,d}")
