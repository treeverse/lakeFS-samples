"""Existing-repository discovery and selection. Never creates or deletes repos."""

from __future__ import annotations

from collections.abc import Callable

from gateway.lakefs_client import LakeFSClient

NO_REPOS_MESSAGE = (
    "No accessible lakeFS repositories were found. Create or obtain access to a "
    "repository in lakeFS Cloud, then run the sample again."
)


class RepositorySelectionError(RuntimeError):
    pass


def list_repository_ids(client: LakeFSClient) -> list[str]:
    return [r["id"] for r in client.list_repositories()]


def select_repository(
    client: LakeFSClient,
    configured: str | None = None,
    chooser: Callable[[list[str]], str] | None = None,
) -> str:
    """Select the existing repository to operate in.

    * configured -> verify it exists.
    * exactly one accessible -> select it.
    * several accessible -> use ``chooser`` (interactive) or error.
    * none accessible -> stop with the required message.
    """
    ids = list_repository_ids(client)
    if configured:
        if configured not in ids:
            raise RepositorySelectionError(
                f"configured repository {configured!r} is not accessible. "
                f"Accessible repositories: {ids or 'none'}"
            )
        return configured
    if not ids:
        raise RepositorySelectionError(NO_REPOS_MESSAGE)
    if len(ids) == 1:
        return ids[0]
    if chooser is None:
        raise RepositorySelectionError(
            "Multiple repositories are accessible. Set LAKEFS_REPOSITORY or run "
            f"interactively to choose one of: {ids}"
        )
    choice = chooser(ids)
    if choice not in ids:
        raise RepositorySelectionError(f"{choice!r} is not an accessible repository")
    return choice
