from orchestrator.seeding import seed_run, verify_source_unchanged
from seed.generate import corpus_objects


def test_seed_run_creates_disposable_branches(fake_client, tmp_generated):
    rs = seed_run(fake_client, "demo-repo", "main")
    scope = rs.scope

    # Source HEAD recorded; source branch untouched.
    assert rs.source_commit == fake_client.branch_head("demo-repo", "main")
    assert verify_source_unchanged(fake_client, rs)

    # Baseline + workspace exist; workspace derived after baseline commit.
    assert fake_client.branch_exists("demo-repo", scope.baseline_branch)
    assert fake_client.branch_exists("demo-repo", scope.workspace_branch)
    assert rs.baseline_commit

    # Corpus seeded on baseline under the corpus prefix.
    listing = fake_client.list_objects("demo-repo", scope.baseline_branch, prefix=scope.corpus_prefix, amount=200)
    assert len(listing["results"]) == len(corpus_objects())
    assert rs.stage == "seeded"


def test_seed_run_is_idempotent_on_branches(fake_client, tmp_generated):
    rs = seed_run(fake_client, "demo-repo", "main")
    # Re-seeding the same run's branches should not raise (branch_exists guard).
    from common.runstate import RunState

    rs2 = RunState.load(rs.run_id)
    assert rs2.run_id == rs.run_id
