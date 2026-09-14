import json
from pathlib import Path

from memequant.config import Settings
from memequant.storage.state import StateStore


def _file_stats(data_dir: Path) -> dict:
    raw = list((data_dir / "raw").glob("date=*/hour=*/transactions.jsonl.gz"))
    parquet = list((data_dir / "normalized").glob("*/*/*.parquet"))
    jsonl = list((data_dir / "normalized").glob("*/*/*.jsonl.gz"))
    return {
        "raw_files": len(raw),
        "raw_bytes": sum(p.stat().st_size for p in raw),
        "parquet_files": len(parquet),
        "normalized_jsonl_files": len(jsonl),
        "normalized_bytes": sum(p.stat().st_size for p in parquet + jsonl),
    }


def main() -> None:
    settings = Settings()
    state = StateStore(settings.state_db_path)
    try:
        output = state.summary() | {"files": _file_stats(settings.data_dir)}
        print(json.dumps(output, indent=2, ensure_ascii=False))
    finally:
        state.close()


if __name__ == "__main__":
    main()
