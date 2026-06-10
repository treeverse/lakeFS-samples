"""Shared plumbing for the lakeFS + E2B agentic demos.

This package holds the helpers that every use case builds on:

- ``lakefs_client``: thin wrappers around the lakeFS high-level Python SDK
  (create/read/write/commit/merge branches, install pre-merge Actions).
- ``env``: small helpers for loading and requiring environment variables.

Use-case-specific logic (validation rules, agent loops, sandbox runners,
reporting) lives inside each ``usecases/<name>`` package, not here.
"""
from __future__ import annotations

from lakefs_e2b_common import env, lakefs_client

__all__ = ["env", "lakefs_client"]
