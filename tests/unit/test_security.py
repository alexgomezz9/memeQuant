import pytest

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
