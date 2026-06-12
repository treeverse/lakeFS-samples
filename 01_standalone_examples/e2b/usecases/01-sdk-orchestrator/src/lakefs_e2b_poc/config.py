"""Load and validate configuration from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

_LOCALHOST_PATTERNS = ("localhost", "127.0.0.1", "host.docker.internal")


@dataclass
class Config:
    e2b_api_key: str
    lakefs_endpoint: str
    lakefs_access_key_id: str
    lakefs_secret_access_key: str
    lakefs_repo: str
    lakefs_main_branch: str
    lakefs_s3_endpoint: str
    lakefs_storage_namespace: str
    keep_branches: bool
    allow_localhost_lakefs: bool


def load_config() -> Config:
    def req(name: str) -> str:
        val = os.environ.get(name, "").strip()
        if not val:
            raise SystemExit(f"ERROR: Required environment variable {name!r} is not set.")
        return val

    lakefs_endpoint = req("LAKEFS_ENDPOINT")
    s3_endpoint = os.environ.get("LAKEFS_S3_ENDPOINT", "").strip() or lakefs_endpoint

    return Config(
        e2b_api_key=req("E2B_API_KEY"),
        lakefs_endpoint=lakefs_endpoint,
        lakefs_access_key_id=req("LAKEFS_ACCESS_KEY_ID"),
        lakefs_secret_access_key=req("LAKEFS_SECRET_ACCESS_KEY"),
        lakefs_repo=req("LAKEFS_REPO"),
        lakefs_main_branch=os.environ.get("LAKEFS_MAIN_BRANCH", "main").strip() or "main",
        lakefs_s3_endpoint=s3_endpoint,
        lakefs_storage_namespace=os.environ.get("LAKEFS_STORAGE_NAMESPACE", "").strip(),
        keep_branches=os.environ.get("KEEP_BRANCHES", "true").strip().lower() != "false",
        allow_localhost_lakefs=os.environ.get("ALLOW_LOCALHOST_LAKEFS", "false").strip().lower() == "true",
    )


def check_endpoint_not_localhost(config: Config) -> None:
    for endpoint in (config.lakefs_endpoint, config.lakefs_s3_endpoint):
        for pattern in _LOCALHOST_PATTERNS:
            if pattern in endpoint and not config.allow_localhost_lakefs:
                raise SystemExit(
                    f"ERROR: lakeFS endpoint '{endpoint}' appears to be localhost.\n\n"
                    "E2B sandboxes run in the cloud and cannot reach your local machine.\n\n"
                    "Options:\n"
                    "  1. Use lakeFS Cloud (https://lakefs.io) or another publicly reachable endpoint.\n"
                    "  2. Expose your local lakeFS via a tunnel (e.g. ngrok, Tailscale) and use "
                    "the tunnel URL.\n"
                    "  3. Set ALLOW_LOCALHOST_LAKEFS=true to skip this check (only useful if the "
                    "sandbox can actually reach the endpoint)."
                )
