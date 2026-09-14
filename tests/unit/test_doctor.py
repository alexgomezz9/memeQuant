from pathlib import Path

from memequant.cli.doctor import _local_checks
from memequant.config import Settings


def test_doctor_local_checks_idls_and_writable_data_dir(tmp_path: Path):
    s = Settings(_env_file=None, MEMEQUANT_DATA_DIR=tmp_path, MEMEQUANT_PROTOCOLS="pump")
    result = _local_checks(s)
    assert result["local_ok"]
    assert result["data_dir_writable"]
    assert result["idls"]["pump"]["program_id"].startswith("6EF8")
    assert "TradeEvent" in result["idls"]["pump"]["events"]
