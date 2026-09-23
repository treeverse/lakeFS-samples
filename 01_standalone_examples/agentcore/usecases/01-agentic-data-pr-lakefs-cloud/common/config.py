"""Configuration loading from `.env` / environment variables.

Two configs: lakeFS Cloud connection details and AWS/AgentCore settings. The
lakeFS secret key is stored in a field whose repr is masked, so it never leaks
into tracebacks, logs, or `print(config)`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

try:  # optional; env vars work without it
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore

SAMPLE_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = SAMPLE_ROOT / ".env"


def load_env(env_path: Path | None = None) -> None:
    """Load `.env` if present. Real environment variables always win."""
    path = env_path or ENV_PATH
    if load_dotenv and path.exists():
        load_dotenv(path, override=False)
    # An empty AWS_PROFILE in the environment makes boto3 raise ProfileNotFound
    # instead of using the default credential chain. Treat empty as unset.
    if not os.environ.get("AWS_PROFILE", "").strip():
        os.environ.pop("AWS_PROFILE", None)


class _Secret:
    """A string whose repr/str never reveals the value."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value or ""

    def reveal(self) -> str:
        return self._value

    def __bool__(self) -> bool:
        return bool(self._value)

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return "***REDACTED***" if self._value else "<empty>"

    __str__ = __repr__


class ConfigError(RuntimeError):
    """Raised when a required configuration value is missing or invalid."""


@dataclass
class LakeFSConfig:
    endpoint: str
    access_key_id: str
    _secret: _Secret = field(repr=False)
    repository: str | None = None
    source_branch: str = "main"

    @property
    def secret_access_key(self) -> str:
        return self._secret.reveal()

    @classmethod
    def from_env(cls) -> LakeFSConfig:
        load_env()
        endpoint = (os.getenv("LAKEFS_ENDPOINT") or "").strip().rstrip("/")
        access_key = (os.getenv("LAKEFS_ACCESS_KEY_ID") or "").strip()
        secret = os.getenv("LAKEFS_SECRET_ACCESS_KEY") or ""
        missing = [
            name
            for name, val in [
                ("LAKEFS_ENDPOINT", endpoint),
                ("LAKEFS_ACCESS_KEY_ID", access_key),
                ("LAKEFS_SECRET_ACCESS_KEY", secret),
            ]
            if not val
        ]
        if missing:
            raise ConfigError(
                "Missing required lakeFS configuration: "
                + ", ".join(missing)
                + ". Run `make configure` or set these environment variables."
            )
        if not endpoint.startswith(("http://", "https://")):
            raise ConfigError(
                f"LAKEFS_ENDPOINT must be an http(s) URL, got {endpoint!r}"
            )
        return cls(
            endpoint=endpoint,
            access_key_id=access_key,
            _secret=_Secret(secret),
            repository=(os.getenv("LAKEFS_REPOSITORY") or "").strip() or None,
            source_branch=(os.getenv("LAKEFS_SOURCE_BRANCH") or "main").strip() or "main",
        )


@dataclass
class AWSConfig:
    region: str
    model_id: str
    resource_prefix: str = "agentcore-data-pr"
    profile: str | None = None

    @classmethod
    def from_env(cls) -> AWSConfig:
        load_env()
        region = (os.getenv("AWS_REGION") or "").strip()
        model_id = (os.getenv("BEDROCK_MODEL_ID") or "").strip()
        missing = [
            name
            for name, val in [("AWS_REGION", region), ("BEDROCK_MODEL_ID", model_id)]
            if not val
        ]
        if missing:
            raise ConfigError(
                "Missing required AWS configuration: "
                + ", ".join(missing)
                + ". Set them in `.env` (see .env.example)."
            )
        return cls(
            region=region,
            model_id=model_id,
            resource_prefix=(os.getenv("AGENTCORE_RESOURCE_PREFIX") or "agentcore-data-pr").strip(),
            profile=(os.getenv("AWS_PROFILE") or "").strip() or None,
        )
