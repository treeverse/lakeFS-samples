"""Environment-variable loading helpers shared across use cases.

Each use case defines its own typed ``Config`` dataclass, but they all read
from the environment the same way: load a ``.env`` file if present, then pull
required values (erroring clearly when one is missing) and optional values
(with defaults). These helpers centralise that pattern.
"""
from __future__ import annotations

import os

from dotenv import find_dotenv, load_dotenv

# Load a .env file searching from the current working directory upward. In the
# monorepo each use case keeps its own .env in its own directory, so we must
# discover relative to the cwd (where the script/command is run), NOT relative
# to this shared module's location.
load_dotenv(find_dotenv(usecwd=True))


def require(name: str) -> str:
    """Return a required environment variable, or exit with a clear message."""
    val = os.environ.get(name, "").strip()
    if not val:
        raise SystemExit(f"ERROR: Required environment variable {name!r} is not set.")
    return val


def optional(name: str, default: str = "") -> str:
    """Return an optional environment variable, falling back to ``default``."""
    val = os.environ.get(name, "").strip()
    return val or default


def optional_int(name: str, default: int) -> int:
    """Return an optional integer environment variable, falling back to ``default``."""
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw else default
