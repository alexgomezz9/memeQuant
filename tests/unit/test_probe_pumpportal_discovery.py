from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from scripts.probe_pumpportal_discovery import (
    API_KEY_ENV,
    SUBSCRIBE_NEW_TOKEN_REQUEST,
    CaptureWriter,
    InvalidJsonMessage,
    build_websocket_url,
    decode_message,
    main,
    redact_probe_error,
    summary_path_for,
    write_summary,
)


def test_capture_preserves_payload_and_reports_observed_schema(tmp_path) -> None:
    output = tmp_path / "discovery.jsonl"
    first_raw = json.dumps(
        {
            "mint": "mint-a",
            "signature": "signature-a",
            "creator": "creator-a",
            "timestamp": 1_700_000_000,
            "details": {"active": True},
        },
        separators=(",", ":"),
    )
    second_raw = json.dumps(
        {
            "mint": "mint-b",
            "signature": "signature-b",
            "user": "user-b",
            "details": {"active": None},
            "additional": 12.5,
        },
        separators=(",", ":"),
    )

    with CaptureWriter(output) as writer:
        writer.append(first_raw, received_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))
        writer.append(second_raw.encode(), received_at=datetime(2026, 1, 2, 3, 4, 6, tzinfo=UTC))
        summary = writer.observer.summary()
        messages_received = writer.messages_received
        bytes_received = writer.bytes_received

    records = [json.loads(line) for line in output.read_text().splitlines()]
    assert set(records[0]) == {"received_at", "payload"}
    assert records[0]["received_at"] == "2026-01-02T03:04:05Z"
    assert records[0]["payload"] == json.loads(first_raw)
    assert records[1]["payload"] == json.loads(second_raw)
    first_line = output.read_bytes().splitlines()[0]
    assert b'"payload":' + first_raw.encode() in first_line
    assert messages_received == 2
    assert bytes_received == len(first_raw.encode()) + len(second_raw.encode())

    fields = {field["path"]: field for field in summary["fields"]}
    assert fields["$/mint"]["always_when_parent_is_object"] is True
    assert fields["$/mint"]["types"] == {"string": 2}
    assert fields["$/creator"]["optional_when_parent_is_object"] is True
    assert fields["$/creator"]["missing"] == 1
    assert fields["$/details/active"]["types"] == {"boolean": 1, "null": 1}
    assert fields["$/additional"]["types"] == {"number": 1}
    assert summary["requested_field_names"]["mint"] == {
        "exists": True,
        "paths": ["$/mint"],
    }
    assert summary["requested_field_names"]["creator"]["exists"] is True
    assert summary["requested_field_names"]["user"]["exists"] is True


def test_capture_refuses_to_overwrite_existing_output(tmp_path) -> None:
    output = tmp_path / "existing.jsonl"
    output.write_text("keep me")

    with pytest.raises(FileExistsError):
        CaptureWriter(output)

    assert output.read_text() == "keep me"


def test_summary_is_written_exclusively(tmp_path) -> None:
    output = tmp_path / "capture.jsonl"
    summary_path = summary_path_for(output)
    summary = {"messages_received": 2, "bytes_received": 42}

    write_summary(summary_path, summary)
    assert json.loads(summary_path.read_text()) == summary
    with pytest.raises(FileExistsError):
        write_summary(summary_path, summary)


def test_decode_message_rejects_invalid_json() -> None:
    with pytest.raises(InvalidJsonMessage, match="not valid JSON"):
        decode_message("not-json")


def test_decode_message_retains_exact_wire_bytes() -> None:
    raw = '  { "mint" : "mint-a" }  '

    payload, raw_bytes = decode_message(raw)

    assert payload == {"mint": "mint-a"}
    assert raw_bytes == raw.encode()


def test_probe_uses_only_subscribe_new_token() -> None:
    assert SUBSCRIBE_NEW_TOKEN_REQUEST == {"method": "subscribeNewToken"}
    assert "subscribeTokenTrade" not in json.dumps(SUBSCRIBE_NEW_TOKEN_REQUEST)


def test_url_and_error_redaction_never_expose_api_key() -> None:
    api_key = "secret/value with spaces"
    url = build_websocket_url(api_key)
    assert url.startswith("wss://pumpportal.fun/api/data?api-key=")

    redacted = redact_probe_error(f"connection failed for {url}; key={api_key}", api_key)
    assert api_key not in redacted
    assert "secret%2Fvalue" not in redacted
    assert "api-key=REDACTED" in redacted


def test_main_requires_only_dedicated_api_key_environment_variable(
    monkeypatch, capsys, tmp_path
) -> None:
    monkeypatch.delenv(API_KEY_ENV, raising=False)

    exit_code = main(["--duration", "30", "--output", str(tmp_path / "probe.jsonl")])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert API_KEY_ENV in captured.err
    assert not (tmp_path / "probe.jsonl").exists()
