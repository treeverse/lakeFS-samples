"""Set up the lakeFS repository and upload the pre-merge action."""
from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from agent_demo.config import load_config
from lakefs_e2b_common.lakefs_client import get_or_create_repo, make_client, upload_lakefsactions_yaml


def main() -> None:
    """Connect to lakeFS, create repo if needed, and upload the pre-merge action."""
    config = load_config()

    print(f"  Connecting to lakeFS at {config.lakefs_endpoint}...")
    client = make_client(
        config.lakefs_endpoint,
        config.lakefs_access_key_id,
        config.lakefs_secret_access_key,
    )

    print(f"  Ensuring repository '{config.lakefs_repository}' exists...")
    repo = get_or_create_repo(client, config.lakefs_repository, config.lakefs_storage_namespace)
    print(f"  Repository '{config.lakefs_repository}' is ready.")

    yaml_path = Path(__file__).parent.parent / "lakefs_actions" / "validate_data.yaml"
    yaml_content = yaml_path.read_text(encoding="utf-8")

    print("  Uploading _lakefs_actions/validate_data.yaml to main branch...")
    upload_lakefsactions_yaml(repo, yaml_content)
    print("  Pre-merge action uploaded and committed.")
    print()
    print("  Setup complete.")
    print(f"    lakeFS repo : {config.lakefs_repository}")
    print(f"    Endpoint    : {config.lakefs_endpoint}")
    print("    Action      : _lakefs_actions/validate_data.yaml (pre-merge gate)")


if __name__ == "__main__":
    main()
