import pytest

from common.naming import RunScope, new_run_id, validate_run_id


def test_run_id_generation_is_branch_safe():
    for _ in range(50):
        rid = new_run_id()
        assert validate_run_id(rid) == rid
        assert "/" not in rid and rid.isalnum()


def test_invalid_run_id_rejected():
    for bad in ["", "UPPER123", "with/slash", "sh", "toolongidentifier123"]:
        with pytest.raises(ValueError):
            validate_run_id(bad)


def test_branch_and_prefix_names():
    s = RunScope(run_id="abcd1234", source_branch="main")
    # Branch ids use dashes (lakeFS forbids slashes in branch ids).
    assert s.baseline_branch == "agentcore-demo-abcd1234-baseline"
    assert s.workspace_branch == "agentcore-demo-abcd1234-workspace"
    # Object prefixes keep slashes (valid in object keys).
    assert s.corpus_prefix == "agentcore-demo/abcd1234/corpus/"
    assert s.output_prefix == "agentcore-demo/abcd1234/output/"
    assert s.curated_us_prefix.startswith(s.output_prefix)
    assert s.quarantine_manifest_path.endswith("quarantine/manifest.json")


def test_write_scope_only_workspace_output():
    s = RunScope(run_id="abcd1234", source_branch="main")
    assert s.writable_branch() == s.workspace_branch
    assert s.is_write_allowed(s.workspace_branch, s.output_prefix + "curated/us/x.json")
    # baseline / source / main are protected; corpus is read-only
    assert not s.is_write_allowed(s.baseline_branch, s.corpus_prefix + "x")
    assert not s.is_write_allowed(s.workspace_branch, s.corpus_prefix + "x")
    assert s.is_protected_branch("main")
    assert s.is_protected_branch(s.baseline_branch)
    assert s.is_protected_branch(s.source_branch)


def test_read_scope():
    s = RunScope(run_id="abcd1234", source_branch="main")
    assert s.is_read_allowed(s.baseline_branch, s.corpus_prefix + "a.json")
    assert not s.is_read_allowed(s.baseline_branch, "some/other/prefix/a")
    assert not s.is_read_allowed("unrelated-branch", s.corpus_prefix + "a")
