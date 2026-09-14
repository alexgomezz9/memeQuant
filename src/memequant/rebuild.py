from __future__ import annotations

import gzip
import shutil
import tempfile
from pathlib import Path

import orjson

from memequant.app import build_engine
from memequant.config import Settings
from memequant.models import RawLogNotification, RawTransactionEnvelope


def raw_files(root: Path):
    paths = list(root.glob("date=*/hour=*/log_notifications.jsonl.gz"))
    paths.extend(root.glob("date=*/hour=*/transactions.jsonl.gz"))
    # Within an hour, streaming logs are canonical when an optional full-transaction
    # enrichment exists for the same signature. State still dedupes either representation.
    yield from sorted(
        paths,
        key=lambda path: (
            str(path.parent),
            0 if path.name == "log_notifications.jsonl.gz" else 1,
        ),
    )


def rebuild_derived(raw_root: Path, output_root: Path, *, batch_size: int = 500) -> dict[str, int]:
    """Rebuild derived datasets deterministically from authoritative RAW.

    Work happens in a temporary directory and is renamed into place only after a full
    successful pass. Raw duplicate rows are deduplicated through a fresh temporary state DB.
    """
    raw_root = raw_root.resolve()
    output_root = output_root.resolve()
    if output_root == raw_root or raw_root in output_root.parents:
        raise ValueError("output_root must not be inside the authoritative raw_root")
    output_root.parent.mkdir(parents=True, exist_ok=True)
    tmp_root = Path(
        tempfile.mkdtemp(prefix=f".{output_root.name}.rebuild-", dir=output_root.parent)
    )
    settings = Settings(
        _env_file=None,
        MEMEQUANT_DATA_DIR=tmp_root,
        MEMEQUANT_PARQUET_BATCH_SIZE=batch_size,
        MEMEQUANT_RAW_FSYNC_EVERY=0,
    )
    engine = build_engine(settings)
    raw_rows = 0
    raw_log_rows = 0
    raw_transaction_rows = 0
    try:
        for path in raw_files(raw_root):
            with gzip.open(path, "rb") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    raw_rows += 1
                    row = orjson.loads(line)
                    if path.name == "log_notifications.jsonl.gz":
                        raw_log_rows += 1
                        notification = RawLogNotification.model_validate(row)
                        engine.process_log(
                            notification, persist_raw=False, update_checkpoint=False
                        )
                    else:
                        raw_transaction_rows += 1
                        env = RawTransactionEnvelope.model_validate(row)
                        env = env.model_copy(update={"source_mode": "replay"})
                        engine.process(env, persist_raw=False, update_checkpoint=False)
        engine.normalized_store.flush()
        summary = engine.state.summary()
        result = {
            "raw_rows": raw_rows,
            "raw_log_rows": raw_log_rows,
            "raw_transaction_rows": raw_transaction_rows,
            "unique_transactions": int(summary["transactions"]),
            "events": int(summary["events"]),
        }
    except Exception:
        engine.close()
        engine.state.close()
        shutil.rmtree(tmp_root, ignore_errors=True)
        raise
    else:
        engine.close()
        engine.state.close()

    derived = tmp_root / "normalized"
    target_tmp = output_root.with_name(output_root.name + ".incoming")
    if target_tmp.exists():
        shutil.rmtree(target_tmp)
    if derived.exists():
        shutil.move(str(derived), str(target_tmp))
    else:
        target_tmp.mkdir(parents=True)
    if output_root.exists():
        shutil.rmtree(output_root)
    target_tmp.replace(output_root)
    shutil.rmtree(tmp_root, ignore_errors=True)
    return result
