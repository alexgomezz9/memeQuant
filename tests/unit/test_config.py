from pathlib import Path

import pytest

from memequant.config import Settings


def test_live_urls_required():
    s = Settings(_env_file=None)
    with pytest.raises(RuntimeError):
        s.require_live_urls()


def test_settings_aliases_and_paths(tmp_path: Path):
    s = Settings(
        _env_file=None,
        SOLANA_HTTP_RPC_URL="https://x",
        SOLANA_WS_RPC_URL="wss://x",
        MEMEQUANT_DATA_DIR=tmp_path,
        MEMEQUANT_MAX_SUPPORTED_TRANSACTION_VERSION=1,
    )
    s.require_live_urls()
    assert s.state_db_path == tmp_path / "state/collector.sqlite3"
    assert s.max_supported_transaction_version == 1


def test_protocol_selection_validation():
    assert Settings(_env_file=None, MEMEQUANT_PROTOCOLS="pump,pumpswap").enabled_protocols == ("pump", "pumpswap")
    with pytest.raises(ValueError):
        _ = Settings(_env_file=None, MEMEQUANT_PROTOCOLS="pump,whatever").enabled_protocols
