"""Thin helpers around the lakeFS high-level Python SDK."""
from __future__ import annotations

from typing import Any

import lakefs
from lakefs.client import Client


def make_client(endpoint: str, access_key_id: str, secret_access_key: str) -> Client:
    return Client(host=endpoint, username=access_key_id, password=secret_access_key)


def get_repo(client: Client, repo_name: str) -> lakefs.Repository:
    return lakefs.repository(repo_name, client=client)


def ensure_repo_exists(client: Client, repo_name: str, storage_namespace: str = "") -> lakefs.Repository:
    """Return the repo, creating it if it doesn't exist and storage_namespace is provided."""
    repo = get_repo(client, repo_name)
    try:
        # Force an API call; raises if the repo doesn't exist
        _ = list(repo.branches())
        return repo
    except Exception as exc:
        err = str(exc).lower()
        if "not found" in err or "404" in err or "no such repository" in err or "repository" in err:
            if not storage_namespace:
                raise SystemExit(
                    f"ERROR: lakeFS repository '{repo_name}' does not exist.\n"
                    "Either create it in the lakeFS UI or set LAKEFS_STORAGE_NAMESPACE "
                    "to allow automatic creation.\n"
                    "  Example: LAKEFS_STORAGE_NAMESPACE=s3://my-bucket/lakefs-data"
                ) from exc
            print(f"  Repository '{repo_name}' not found. Creating with namespace '{storage_namespace}'...")
            return repo.create(storage_namespace=storage_namespace)
        raise


def create_branch(repo: lakefs.Repository, branch_name: str, source_ref: str) -> lakefs.Branch:
    return repo.branch(branch_name).create(source_reference=source_ref)


def list_uncommitted(branch: lakefs.Branch) -> list[Any]:
    return list(branch.uncommitted())


def commit_branch(branch: lakefs.Branch, message: str, metadata: dict[str, str]) -> Any:
    return branch.commit(message=message, metadata=metadata)


def merge_into_main(feature_branch: lakefs.Branch, main_branch: lakefs.Branch) -> Any:
    return feature_branch.merge_into(main_branch)


def delete_branch(branch: lakefs.Branch) -> None:
    branch.delete()
