"""Thin wrappers around the lakeFS high-level Python SDK."""
from __future__ import annotations

import lakefs
from lakefs.client import Client


def make_client(endpoint: str, access_key_id: str, secret_access_key: str) -> Client:
    """Create and return a lakeFS Client."""
    return Client(host=endpoint, username=access_key_id, password=secret_access_key)


def get_or_create_repo(
    client: Client, repo_name: str, storage_namespace: str = ""
) -> lakefs.Repository:
    """Return the repository, creating it if it doesn't exist and storage_namespace is given."""
    repo = lakefs.repository(repo_name, client=client)
    try:
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
                    "  Example: LAKEFS_STORAGE_NAMESPACE=s3://my-bucket/data-agent-demo/"
                ) from exc
            print(f"  Repository '{repo_name}' not found. Creating with namespace '{storage_namespace}'...")
            return repo.create(storage_namespace=storage_namespace)
        raise


def create_branch(
    repo: lakefs.Repository, branch_name: str, source_branch: str
) -> lakefs.Branch:
    """Create and return a new branch from source_branch."""
    return repo.branch(branch_name).create(source_reference=source_branch)


def read_object(branch: lakefs.Branch, path: str) -> str:
    """Read an object from the branch and return its text content."""
    obj = branch.object(path)
    data = obj.reader().read()
    if isinstance(data, bytes):
        return data.decode("utf-8")
    return data


def write_object(branch: lakefs.Branch, path: str, content: str | bytes) -> None:
    """Write content to an object on the branch."""
    obj = branch.object(path)
    if isinstance(content, str):
        content = content.encode("utf-8")
    with obj.writer() as w:
        w.write(content)


def commit(branch: lakefs.Branch, message: str, metadata: dict[str, str]) -> str:
    """Commit staged changes and return the commit ID."""
    result = branch.commit(message=message, metadata=metadata)
    return result.get_commit().id


def merge_branch(feature_branch: lakefs.Branch, main_branch_obj: lakefs.Branch) -> str:
    """Merge feature_branch into main_branch_obj and return the merge ref."""
    result = feature_branch.merge_into(main_branch_obj)
    return str(result)


def list_branches(repo: lakefs.Repository) -> list[str]:
    """Return a list of branch names for the repository."""
    return [b.id for b in repo.branches()]


def list_commits(branch: lakefs.Branch, max_amount: int = 10) -> list[dict]:
    """Return up to max_amount recent commits as dicts."""
    commits = []
    for c in branch.log(max_amount=max_amount):
        commit_obj = c.get_commit() if hasattr(c, "get_commit") else c
        commits.append(
            {
                "id": getattr(commit_obj, "id", str(c)),
                "message": getattr(commit_obj, "message", ""),
                "committer": getattr(commit_obj, "committer", ""),
            }
        )
    return commits


def upload_lakefsactions_yaml(
    repo: lakefs.Repository,
    yaml_content: str,
    filename: str = "validate_data.yaml",
) -> None:
    """Upload a pre-merge action to ``_lakefs_actions/<filename>`` on main and commit.

    Idempotent: if the action is already present unchanged, the commit is a no-op rather
    than an error (so setup can be re-run safely).
    """
    main_branch = repo.branch("main")
    write_object(main_branch, f"_lakefs_actions/{filename}", yaml_content)
    try:
        commit(
            main_branch,
            message=f"Add/update pre-merge action ({filename})",
            metadata={"created_by": "setup_script"},
        )
    except Exception as exc:  # already installed & unchanged -> nothing to commit
        if "no changes" in str(exc).lower():
            return
        raise
