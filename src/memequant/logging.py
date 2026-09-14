import json
import logging
import sys
from datetime import UTC, datetime

from memequant.security import redact_text, redact_value


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": redact_text(record.getMessage()),
        }
        for key in (
            "program",
            "slot",
            "signature",
            "event_type",
            "count",
            "mode",
            "error_code",
        ):
            if hasattr(record, key):
                payload[key] = redact_value(getattr(record, key))
        if record.exc_info:
            payload["exception"] = redact_text(self.formatException(record.exc_info))
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
    # Avoid logging full RPC URLs (which often embed API keys) for every successful call.
    logging.getLogger("httpx").setLevel(logging.WARNING)
