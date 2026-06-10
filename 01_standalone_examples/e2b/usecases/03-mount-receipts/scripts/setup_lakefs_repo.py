"""Create the lakeFS repo (if needed) and install the pre-merge ledger gate on main."""
from __future__ import annotations

from pathlib import Path

from lakefs_e2b_common.lakefs_client import get_or_create_repo, make_client, upload_lakefsactions_yaml
from mount_receipts.config import load_config


def main() -> None:
    cfg = load_config()
    print(f"  Connecting to lakeFS at {cfg.lakefs_endpoint}...")
    client = make_client(cfg.lakefs_endpoint, cfg.lakefs_access_key_id, cfg.lakefs_secret_access_key)

    print(f"  Ensuring repository '{cfg.lakefs_repository}' exists...")
    repo = get_or_create_repo(client, cfg.lakefs_repository, cfg.lakefs_storage_namespace)
    print(f"  Repository '{cfg.lakefs_repository}' is ready.")

    yaml_path = Path(__file__).parent.parent / "lakefs_actions" / "validate_ledger.yaml"
    print("  Uploading _lakefs_actions/validate_ledger.yaml to main...")
    upload_lakefsactions_yaml(repo, yaml_path.read_text(encoding="utf-8"), filename="validate_ledger.yaml")
    print("  Pre-merge gate installed.")


if __name__ == "__main__":
    main()
