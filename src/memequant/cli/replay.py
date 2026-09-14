import argparse
import json
from pathlib import Path

from memequant.config import Settings
from memequant.rebuild import rebuild_derived


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Deterministically rebuild research datasets from authoritative RAW"
    )
    parser.add_argument("--raw", type=Path, default=None, help="RAW root (default: data/raw)")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Derived output (default: data/rebuilt_normalized)",
    )
    args = parser.parse_args()
    settings = Settings()
    raw = args.raw or settings.data_dir / "raw"
    output = args.output or settings.data_dir / "rebuilt_normalized"
    result = rebuild_derived(raw, output, batch_size=settings.parquet_batch_size)
    print(json.dumps(result | {"output": str(output)}, indent=2))


if __name__ == "__main__":
    main()
