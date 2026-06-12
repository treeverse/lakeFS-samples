"""Load and validate configuration from environment variables."""
from __future__ import annotations

from dataclasses import dataclass

from lakefs_e2b_common.env import optional, optional_int, require


@dataclass
class Config:
    openai_api_key: str
    openai_model: str
    e2b_api_key: str
    e2b_team: str        # optional — enables sandbox links in lakeFS commit metadata
    lakefs_endpoint: str
    lakefs_access_key_id: str
    lakefs_secret_access_key: str
    lakefs_repository: str
    lakefs_storage_namespace: str
    lakefs_branch: str
    max_attempts: int


def load_config() -> Config:
    """Read config from environment, raising SystemExit for missing required vars."""
    return Config(
        openai_api_key=require("OPENAI_API_KEY"),
        openai_model=optional("OPENAI_MODEL", "gpt-4o-mini"),
        e2b_api_key=require("E2B_API_KEY"),
        e2b_team=optional("E2B_TEAM"),
        lakefs_endpoint=require("LAKEFS_ENDPOINT"),
        lakefs_access_key_id=require("LAKEFS_ACCESS_KEY_ID"),
        lakefs_secret_access_key=require("LAKEFS_SECRET_ACCESS_KEY"),
        lakefs_repository=require("LAKEFS_REPOSITORY"),
        lakefs_storage_namespace=optional("LAKEFS_STORAGE_NAMESPACE"),
        lakefs_branch=optional("LAKEFS_BRANCH", "main"),
        max_attempts=optional_int("MAX_ATTEMPTS", 5),
    )
