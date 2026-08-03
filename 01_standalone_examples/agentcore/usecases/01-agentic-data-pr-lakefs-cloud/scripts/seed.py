"""`make seed` -- create disposable baseline + workspace branches and seed data."""

from __future__ import annotations

import sys

from common.config import LakeFSConfig
from orchestrator.context import lakefs_client_from_env
from orchestrator.repository import RepositorySelectionError, select_repository
from orchestrator.seeding import seed_run


def _interactive_chooser(repos: list[str]) -> str:
    print("Multiple repositories are accessible:")
    for i, r in enumerate(repos, 1):
        print(f"  {i}. {r}")
    choice = input(f"Select [1-{len(repos)}]: ").strip()
    return repos[int(choice) - 1] if choice.isdigit() else ""


def main() -> int:
    client, cfg = lakefs_client_from_env()
    chooser = _interactive_chooser if sys.stdin.isatty() else None
    try:
        repository = select_repository(client, cfg.repository, chooser)
    except RepositorySelectionError as exc:
        print(f"ERROR: {exc}")
        return 2

    print(f"Repository: {repository}")
    print(f"Source branch: {cfg.source_branch}")
    print("Seeding disposable baseline + workspace branches...\n")

    rs = seed_run(client, repository, cfg.source_branch)
    scope = rs.scope
    print(f"Run id:            {rs.run_id}")
    print(f"Source commit:     {rs.source_commit}")
    print(f"Baseline branch:   {scope.baseline_branch} @ {rs.baseline_commit}")
    print(f"Workspace branch:  {scope.workspace_branch}")
    print(f"Corpus prefix:     {scope.corpus_prefix}")
    print(f"\nRun state saved to generated/{rs.run_id}.json")
    print("Next: `make deploy` then `make run`, or `make run-local` (no AWS).")
    return 0


if __name__ == "__main__":
    # Surface config errors nicely.
    try:
        LakeFSConfig.from_env()
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}")
        raise SystemExit(2) from None
    raise SystemExit(main())
