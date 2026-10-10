"""Create the disposable baseline + workspace branches and seed the corpus.

Never writes to the source branch. Records the source HEAD first, derives the
baseline from that exact commit, seeds the synthetic corpus, commits, and derives
the (zero-copy) workspace branch from the baseline commit.
"""

from __future__ import annotations

from common.runstate import RunState
from gateway.lakefs_client import LakeFSClient
from seed.generate import corpus_objects


def seed_run(
    client: LakeFSClient, repository: str, source_branch: str = "main"
) -> RunState:
    # 1. Record the selected source branch HEAD (immutability anchor).
    source_commit = client.branch_head(repository, source_branch)
    rs = RunState.create(repository, source_branch)
    rs.source_commit = source_commit
    scope = rs.scope
    rs.save()

    # 2. Create the baseline branch from that exact commit (idempotent).
    if not client.branch_exists(repository, scope.baseline_branch):
        client.create_branch(repository, scope.baseline_branch, source_commit)
    rs.record_lakefs_resource(type="branch", id=scope.baseline_branch)

    # 3. Seed the synthetic corpus under the corpus prefix on the baseline.
    for doc in corpus_objects():
        client.upload_object(
            repository,
            scope.baseline_branch,
            scope.corpus_prefix + doc.relpath,
            doc.content(),
            doc.content_type,
        )

    # 4. Commit the seeded corpus. 5. Record the baseline commit.
    commit = client.commit(
        repository,
        scope.baseline_branch,
        "agentcore-demo: seed synthetic support corpus",
        {"run_id": rs.run_id, "stage": "seed-corpus"},
    )
    rs.baseline_commit = commit["id"]

    # 6. Create the agent workspace branch from the baseline commit (zero-copy).
    if not client.branch_exists(repository, scope.workspace_branch):
        client.create_branch(repository, scope.workspace_branch, rs.baseline_commit)
    rs.record_lakefs_resource(type="branch", id=scope.workspace_branch)

    rs.stage = "seeded"
    rs.save()
    return rs


def verify_source_unchanged(client: LakeFSClient, rs: RunState) -> bool:
    """Verify the selected source branch HEAD still matches the recorded commit."""
    current = client.branch_head(rs.repository, rs.source_branch)
    return current == rs.source_commit
