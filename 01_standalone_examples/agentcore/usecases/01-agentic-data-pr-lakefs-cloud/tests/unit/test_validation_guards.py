import pytest

from common.naming import RunScope
from gateway import validation as v
from gateway.validation import CapabilityError

SCOPE = RunScope(run_id="abcd1234", source_branch="main")


@pytest.mark.parametrize("bad", ["../etc/passwd", "a/../../b", "/abs/path", "a\\b", "x/..", "%2e%2e/x"])
def test_path_traversal_rejected(bad):
    with pytest.raises(CapabilityError) as e:
        v.reject_path_traversal(bad)
    assert e.value.code in {"path_traversal", "invalid_path"}


def test_prefix_enforcement_write():
    good = SCOPE.output_prefix + "curated/us/x.json"
    v.ensure_write_allowed(SCOPE, SCOPE.workspace_branch, good)
    with pytest.raises(CapabilityError):
        v.ensure_write_allowed(SCOPE, SCOPE.workspace_branch, SCOPE.corpus_prefix + "x")


def test_write_to_protected_branch_rejected():
    for branch in (SCOPE.baseline_branch, "main", SCOPE.source_branch):
        with pytest.raises(CapabilityError) as e:
            v.ensure_write_allowed(SCOPE, branch, SCOPE.output_prefix + "x")
        assert e.value.code in {"protected_branch", "branch_not_writable"}


def test_read_prefix_enforcement():
    v.ensure_read_allowed(SCOPE, SCOPE.baseline_branch, SCOPE.corpus_prefix + "a.json")
    with pytest.raises(CapabilityError):
        v.ensure_read_allowed(SCOPE, SCOPE.baseline_branch, "unrelated/a")
    with pytest.raises(CapabilityError):
        v.ensure_read_allowed(SCOPE, "other-branch", SCOPE.corpus_prefix + "a")


def test_object_size_limit():
    v.ensure_size("x" * 100)
    with pytest.raises(CapabilityError) as e:
        v.ensure_size("x" * (v.MAX_OBJECT_SIZE_BYTES + 1))
    assert e.value.code == "object_too_large"


def test_content_type_validation():
    assert v.ensure_content_type("application/json") == "application/json"
    assert v.ensure_content_type("text/markdown; charset=utf-8") == "text/markdown"
    with pytest.raises(CapabilityError):
        v.ensure_content_type("application/x-msdownload")


def test_clamp_amount():
    assert v.clamp_amount(None) == v.DEFAULT_LIST_RESULTS
    assert v.clamp_amount(10) == 10
    assert v.clamp_amount(10_000) == v.MAX_LIST_RESULTS
