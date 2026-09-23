"""Teardown regressions found by tearing down a real deployment.

Three defects made `make cleanup` leak the AgentCore Runtime and then report
alarming failures on a re-run. Each is pinned here.
"""

from __future__ import annotations

import pytest

from scripts import cleanup, deploy


class _Err(Exception):
    """Stand-in for a botocore ClientError subclass, matched by class name."""


class ConflictException(_Err):
    pass


class ResourceNotFoundException(_Err):
    pass


class AccessDeniedException(_Err):
    pass


# --- _delete_with_retry ---------------------------------------------------


def test_retries_while_a_dependency_is_still_tearing_down():
    """A parent delete that fails because children linger must be retried."""
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise ConflictException("still contains 2 policies")

    cleanup._delete_with_retry(flaky, attempts=5, delay=0)
    assert len(calls) == 3


def test_already_deleted_is_success_not_failure():
    """Re-running cleanup must not report gone resources as failures."""

    def gone():
        raise ResourceNotFoundException("not found")

    cleanup._delete_with_retry(gone, attempts=3, delay=0)  # must not raise


def test_non_retryable_error_propagates():
    class SomethingElse(_Err):
        pass

    with pytest.raises(SomethingElse):
        cleanup._delete_with_retry(
            lambda: (_ for _ in ()).throw(SomethingElse("nope")), attempts=3, delay=0
        )


def test_retry_gives_up_and_reports_after_exhausting_attempts():
    calls = []

    def never_ok():
        calls.append(1)
        raise ConflictException("still busy")

    with pytest.raises(ConflictException):
        cleanup._delete_with_retry(never_ok, attempts=3, delay=0)
    assert len(calls) == 3


# --- runtime identity -----------------------------------------------------


class _FakeAgentCore:
    def __init__(self, runtimes):
        self._runtimes = runtimes
        self.deleted = []

    def list_agent_runtimes(self):
        return {"agentRuntimes": self._runtimes}

    def delete_agent_runtime(self, agentRuntimeId):  # noqa: N803
        self.deleted.append(agentRuntimeId)


RUNTIME = {
    "agentRuntimeId": "agentcore_data_pr_abcd1234_agent-5SsWpv2pUJ",
    "agentRuntimeName": "agentcore_data_pr_abcd1234_agent",
    "agentRuntimeArn": "arn:aws:bedrock-agentcore:us-east-1:1:runtime/agentcore_data_pr_abcd1234_agent-5SsWpv2pUJ",
}


def test_runtime_id_is_resolved_from_the_control_plane_not_the_prefix():
    """The resource prefix is not a valid agentRuntimeId.

    Recording the prefix (or an ARN truncated out of CLI stdout) left the runtime
    undeletable, because DeleteAgentRuntime answers AccessDenied for an id that
    does not exist.
    """
    ac = _FakeAgentCore([RUNTIME])
    rid, arn = deploy._resolve_runtime(ac, "agentcore-data-pr-abcd1234")
    assert rid == RUNTIME["agentRuntimeId"]
    assert arn == RUNTIME["agentRuntimeArn"]
    # The prefix must never be mistaken for the id.
    assert rid != "agentcore-data-pr-abcd1234"
    # And the ARN must not be truncated mid-identifier.
    assert arn.endswith(RUNTIME["agentRuntimeId"])


def test_resolve_runtime_returns_none_when_the_agent_was_never_deployed():
    assert deploy._resolve_runtime(_FakeAgentCore([]), "agentcore-data-pr-abcd1234") == (
        None,
        None,
    )


def test_agent_name_matches_what_the_agentcore_cli_is_told():
    """`_resolve_runtime` matches on this name, so it must equal the configured one."""
    assert deploy._runtime_agent_name("agentcore-data-pr-abcd1234") == (
        "agentcore_data_pr_abcd1234_agent"
    )
    assert len(deploy._runtime_agent_name("agentcore-data-pr-abcd1234")) <= 48


def test_absent_runtime_is_not_deleted_and_not_retried():
    """AgentCore masks a missing runtime as AccessDenied; don't retry for 30s."""
    ac = _FakeAgentCore([])
    assert cleanup._runtime_exists(ac, RUNTIME["agentRuntimeId"]) is False
    assert ac.deleted == []


def test_present_runtime_is_reported_as_existing():
    ac = _FakeAgentCore([RUNTIME])
    assert cleanup._runtime_exists(ac, RUNTIME["agentRuntimeId"]) is True


def test_unlistable_runtime_falls_through_to_the_delete_attempt():
    """If we can't tell, let the delete speak rather than silently skipping it."""

    class _Broken:
        def list_agent_runtimes(self):
            raise AccessDeniedException("no list permission")

    assert cleanup._runtime_exists(_Broken(), RUNTIME["agentRuntimeId"]) is True


# --- memory store ---------------------------------------------------------


class _MemAgentCore:
    def __init__(self, memories):
        self._memories = memories
        self.deleted = []

    def list_memories(self):
        return {"memories": self._memories}

    def delete_memory(self, memoryId):  # noqa: N803
        self.deleted.append(memoryId)


class _RS:
    def __init__(self, run_id):
        self.run_id = run_id


def test_toolkit_created_memory_store_is_cleaned_up():
    """The agentcore CLI creates this store, so it is absent from aws_resources."""
    ac = _MemAgentCore([{"id": "agentcore_data_pr_abcd1234_agent_mem-LOFaxuAG29"}])
    failures: list[str] = []
    cleanup._cleanup_agentcore_memory(_RS("abcd1234"), ac, failures)
    assert ac.deleted == ["agentcore_data_pr_abcd1234_agent_mem-LOFaxuAG29"]
    assert failures == []


def test_memory_cleanup_never_touches_another_run():
    ac = _MemAgentCore(
        [
            {"id": "agentcore_data_pr_abcd1234_agent_mem-AAA"},
            {"id": "agentcore_data_pr_99998888_agent_mem-BBB"},
            {"id": "someone-elses-memory-CCC"},
        ]
    )
    cleanup._cleanup_agentcore_memory(_RS("abcd1234"), ac, [])
    assert ac.deleted == ["agentcore_data_pr_abcd1234_agent_mem-AAA"]


def test_memory_list_failure_is_reported_not_swallowed():
    class _Broken:
        def list_memories(self):
            raise AccessDeniedException("denied")

    failures: list[str] = []
    cleanup._cleanup_agentcore_memory(_RS("abcd1234"), _Broken(), failures)
    assert len(failures) == 1 and "memory" in failures[0]
