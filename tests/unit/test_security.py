import json
import logging

import pytest

from memequant.cli import collector
from memequant.ingestion.reconcile import ReconciliationLimitExceeded
from memequant.logging import JsonFormatter
from memequant.security import redact_known_secrets, redact_rpc_url, validate_secure_rpc_url


def test_secure_rpc_urls_require_tls():
    assert validate_secure_rpc_url("https://rpc.example", websocket=False)
    assert validate_secure_rpc_url("wss://rpc.example", websocket=True)
    with pytest.raises(ValueError):
        validate_secure_rpc_url("http://rpc.example", websocket=False)
    with pytest.raises(ValueError):
        validate_secure_rpc_url("ws://rpc.example", websocket=True)


def test_rpc_url_rejects_userinfo_credentials():
    with pytest.raises(ValueError):
        validate_secure_rpc_url("https://user:pass@rpc.example", websocket=False)


def test_redact_helius_query_key():
    url = "https://mainnet.helius-rpc.com/?api-key=supersecret"
    safe = redact_rpc_url(url)
    assert "supersecret" not in safe
    assert "REDACTED" in safe


def test_redact_alchemy_path_key():
    url = "wss://solana-mainnet.streaming.alchemy.com/v2/supersecret"
    safe = redact_rpc_url(url)
    assert "supersecret" not in safe
    assert safe.endswith("/v2/REDACTED")


def test_redact_known_secrets_from_exception_text():
    http = "https://mainnet.helius-rpc.com/?api-key=abc123"
    ws = "wss://solana-mainnet.streaming.alchemy.com/v2/def456"
    text = f"failed {http}; secondary key def456"
    safe = redact_known_secrets(text, http, ws)
    assert "abc123" not in safe
    assert "def456" not in safe


def test_generic_log_redaction_hides_query_api_key():
    from memequant.security import redact_text

    text = 'POST https://mainnet.helius-rpc.com/?api-key=secret-123 "HTTP/1.1 429"'
    safe = redact_text(text)
    assert "secret-123" not in safe
    assert "api-key=REDACTED" in safe


def test_json_formatter_redacts_message_structured_fields_and_traceback():
    key = "secret-structured-123"
    url = f"https://mainnet.helius-rpc.com/?api-key={key}"
    try:
        raise RuntimeError(f"request failed for {url}")
    except RuntimeError:
        record = logging.LogRecord(
            "test",
            logging.ERROR,
            __file__,
            1,
            "failed %s",
            (url,),
            __import__("sys").exc_info(),
        )
    record.error_code = {"provider_error": url}
    payload = json.loads(JsonFormatter().format(record))
    rendered = json.dumps(payload)
    assert key not in rendered
    assert "api-key=REDACTED" in rendered


def test_collector_uncaught_startup_error_has_no_secret_traceback(monkeypatch, caplog):
    key = "startup-secret-456"
    url = f"https://mainnet.helius-rpc.com/?api-key={key}"

    def fail(coro):
        coro.close()
        raise RuntimeError(f"could not initialize {url}")

    monkeypatch.setattr(collector.asyncio, "run", fail)
    with caplog.at_level(logging.CRITICAL), pytest.raises(SystemExit) as raised:
        collector.main()
    assert raised.value.code == 1
    assert key not in caplog.text
    assert "api-key=REDACTED" in caplog.text


def test_collector_reconciliation_failure_exits_with_code_2(monkeypatch, caplog):
    def fail(coro):
        coro.close()
        raise ReconciliationLimitExceeded("queue full")

    monkeypatch.setattr(collector.asyncio, "run", fail)
    with caplog.at_level(logging.CRITICAL), pytest.raises(SystemExit) as raised:
        collector.main()
    assert raised.value.code == 2
    assert "collector stopped" in caplog.text
