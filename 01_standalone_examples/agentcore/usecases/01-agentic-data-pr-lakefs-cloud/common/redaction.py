"""Secret redaction for logs and any surface the demo prints.

Nothing in this sample -- logs, UI, CLI output, Lambda CloudWatch lines -- may
reveal the lakeFS secret key, Authorization headers, or Basic-auth tokens.
Redaction is centralised here and unit tested.
"""

from __future__ import annotations

import re
from typing import Any

REDACTED = "***REDACTED***"

# Keys whose values must never be printed, matched case-insensitively.
_SENSITIVE_KEYS = {
    "authorization",
    "secret",
    "secret_access_key",
    "lakefs_secret_access_key",
    "secretaccesskey",
    "password",
    "token",
    "access_key_secret",
    "x-amz-security-token",
}

# Inline patterns (Authorization headers, Basic tokens) redacted from free text.
_PATTERNS = [
    re.compile(r"(Authorization:\s*)(\S+)", re.IGNORECASE),
    re.compile(r"(Basic\s+)([A-Za-z0-9+/=]{8,})"),
    re.compile(r"(Bearer\s+)([A-Za-z0-9._\-]{8,})"),
]


def _is_sensitive_key(key: str) -> bool:
    k = key.lower().replace("-", "_")
    return any(s.replace("-", "_") in k for s in _SENSITIVE_KEYS)


def redact_text(text: str) -> str:
    for pat in _PATTERNS:
        text = pat.sub(lambda m: m.group(1) + REDACTED, text)
    return text


def redact(value: Any) -> Any:
    """Recursively redact sensitive keys in dicts/lists and inline secrets in text."""
    if isinstance(value, dict):
        return {
            k: (REDACTED if _is_sensitive_key(str(k)) else redact(v))
            for k, v in value.items()
        }
    if isinstance(value, (list, tuple)):
        return type(value)(redact(v) for v in value)
    if isinstance(value, str):
        return redact_text(value)
    return value
