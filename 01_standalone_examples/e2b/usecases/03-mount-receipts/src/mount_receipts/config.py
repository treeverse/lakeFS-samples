"""Configuration for the Mount Receipts demo, loaded from the environment."""
from __future__ import annotations

from dataclasses import dataclass

from lakefs_e2b_common.env import optional, require


@dataclass
class Config:
    # lakeFS (Cloud / Enterprise — Mount requires Enterprise)
    lakefs_endpoint: str
    lakefs_access_key_id: str
    lakefs_secret_access_key: str
    lakefs_repository: str
    lakefs_storage_namespace: str
    lakefs_branch: str
    # E2B
    e2b_api_key: str
    e2b_team: str
    e2b_template: str          # optional prebaked template name; "" = default + runtime install
    # OpenAI vision
    openai_api_key: str
    openai_model: str
    # demo controls
    demo_today: str            # pin "today" for reproducible date checks; "" = real today

    def lakectl_envs(self) -> dict[str, str]:
        """Env vars everest/lakectl read inside the sandbox (creds never passed as CLI flags)."""
        return {
            "LAKECTL_SERVER_ENDPOINT_URL": self.lakefs_endpoint,
            "LAKECTL_CREDENTIALS_ACCESS_KEY_ID": self.lakefs_access_key_id,
            "LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY": self.lakefs_secret_access_key,
        }

    def sandbox_envs(self) -> dict[str, str]:
        """Full env passed to the sandbox: lakeFS creds + OpenAI + demo controls."""
        envs = self.lakectl_envs()
        envs["OPENAI_API_KEY"] = self.openai_api_key
        envs["OPENAI_MODEL"] = self.openai_model
        if self.demo_today:
            envs["DEMO_TODAY"] = self.demo_today
        return envs


def load_config() -> Config:
    return Config(
        lakefs_endpoint=require("LAKEFS_ENDPOINT"),
        lakefs_access_key_id=require("LAKEFS_ACCESS_KEY_ID"),
        lakefs_secret_access_key=require("LAKEFS_SECRET_ACCESS_KEY"),
        lakefs_repository=require("LAKEFS_REPOSITORY"),
        lakefs_storage_namespace=optional("LAKEFS_STORAGE_NAMESPACE"),
        lakefs_branch=optional("LAKEFS_BRANCH", "main"),
        e2b_api_key=require("E2B_API_KEY"),
        e2b_team=optional("E2B_TEAM"),
        e2b_template=optional("E2B_TEMPLATE"),
        openai_api_key=require("OPENAI_API_KEY"),
        openai_model=optional("OPENAI_MODEL", "gpt-4o"),
        demo_today=optional("DEMO_TODAY"),
    )
