from pathlib import Path

from memequant.config import Settings
from memequant.engine import IngestionEngine
from memequant.protocols.decoder import default_decoders
from memequant.storage.normalized import NormalizedStore
from memequant.storage.raw import RawJsonlGzipStore, RawLogJsonlGzipStore
from memequant.storage.state import StateStore


def build_engine(settings: Settings, repo_root: Path | None = None) -> IngestionEngine:
    return IngestionEngine(
        state=StateStore(settings.state_db_path),
        raw_store=RawJsonlGzipStore(
            settings.data_dir / "raw", fsync_every=settings.raw_fsync_every
        ),
        raw_log_store=RawLogJsonlGzipStore(
            settings.data_dir / "raw", fsync_every=settings.raw_fsync_every
        ),
        normalized_store=NormalizedStore(
            settings.data_dir / "normalized", batch_size=settings.parquet_batch_size
        ),
        decoders=default_decoders(repo_root),
    )
