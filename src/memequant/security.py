from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qsl, urlsplit, urlunsplit

_SECRET_QUERY_KEYS = {
    "api-key",
    "apikey",
    "api_key",
    "key",
    "token",
    "access_token",
}


def validate_secure_rpc_url(url: str, *, websocket: bool) -> str:
    """Reject plaintext or malformed live RPC endpoints.

    API keys are commonly embedded in the URL, so allowing http/ws would expose them in
    transit. Local test fixtures should continue to use https/wss dummy URLs.
    """
    parsed = urlsplit(url)
    expected = "wss" if websocket else "https"
    if parsed.scheme.lower() != expected:
        raise ValueError(f"RPC URL must use {expected}://")
    if not parsed.hostname:
        raise ValueError("RPC URL must include a hostname")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("RPC URL must not use URL userinfo credentials")
    return url


def redact_rpc_url(url: str) -> str:
    """Return a display-safe RPC URL with likely credentials removed."""
    parsed = urlsplit(url)
    query_items = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        safe_value = "REDACTED" if key.lower() in _SECRET_QUERY_KEYS else value
        query_items.append((key, safe_value))

    # Preserve query order without importing urlencode's '+' space normalization concerns.
    query = "&".join(f"{k}={v}" for k, v in query_items)

    path_parts = parsed.path.split("/")
    # Alchemy-style endpoints place the credential in /v2/<key>.
    if len(path_parts) >= 3 and path_parts[-2].lower() == "v2" and path_parts[-1]:
        path_parts[-1] = "REDACTED"
    path = "/".join(path_parts)
    return urlunsplit((parsed.scheme, parsed.netloc, path, query, parsed.fragment))


def redact_known_secrets(text: str, *urls: str | None) -> str:
    """Scrub credentials derived from configured URLs out of arbitrary error text."""
    out = text
    for url in urls:
        if not url:
            continue
        parsed = urlsplit(url)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True):
            if key.lower() in _SECRET_QUERY_KEYS and value:
                out = out.replace(value, "REDACTED")
        parts = parsed.path.split("/")
        if len(parts) >= 3 and parts[-2].lower() == "v2" and parts[-1]:
            out = out.replace(parts[-1], "REDACTED")
        out = out.replace(url, redact_rpc_url(url))
    return out


_GENERIC_QUERY_SECRET_RE = re.compile(
    r"(?i)([?&](?:api-key|apikey|api_key|key|token|access_token)=)([^&#\s\"\']+)"
)
_GENERIC_V2_SECRET_RE = re.compile(r"(?i)(/v2/)([^/?#\s\"\']+)")


def redact_text(text: str) -> str:
    """Best-effort redaction for credentials embedded in logged URLs.

    This is intentionally generic because third-party loggers such as httpx may render
    a request URL without giving us access to the configured Settings object.
    """
    out = _GENERIC_QUERY_SECRET_RE.sub(r"\1REDACTED", str(text))
    out = _GENERIC_V2_SECRET_RE.sub(r"\1REDACTED", out)
    return out


def redact_value(value: Any) -> Any:
    """Recursively redact strings used as structured logging fields."""
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, dict):
        return {key: redact_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_value(item) for item in value)
    return value
