"""`make cleanup` -- tear down everything this run created, and nothing else.

Walks the run-state resource manifests in reverse. Preserves the lakeFS
repository, the source branch, and every object/branch outside the run's
namespace. Verifies the source branch HEAD is unchanged and reports anything it
could not delete.

If lakeFS retains closed/merged Pull Request history, that record remains as
audit history -- it is intentionally not purged.
"""

from __future__ import annotations

import argparse
import time

from common.runstate import RunState
from orchestrator.context import aws_session, lakefs_client_from_env
from orchestrator.seeding import verify_source_unchanged

# AgentCore deletes are asynchronous: a parent can still report its children as
# present for several seconds after they were deleted successfully. Observed on a
# real teardown as ValidationException ("gateway has targets"), ConflictException
# ("policy engine still contains 2 policies"), and -- misleadingly -- a transient
# AccessDeniedException on DeleteAgentRuntime that succeeded on retry. Retrying is
# what makes a single `make cleanup` sufficient instead of needing a second pass.
_RETRYABLE = (
    "ValidationException",
    "ConflictException",
    "AccessDeniedException",
    "ResourceInUseException",
    "ThrottlingException",
)

# Cleanup must be re-runnable: a partially-completed teardown is the normal reason
# to run it again. An already-deleted resource is a success, not a failure -- without
# this, a second `make cleanup` prints a wall of alarming errors and exits non-zero.
_ALREADY_GONE = (
    "ResourceNotFoundException",
    "NoSuchEntityException",
    "NotFoundException",
)


def _delete_with_retry(fn, *, attempts: int = 6, delay: float = 5.0) -> None:
    """Call a delete, retrying while it fails for a reason teardown resolves.

    Returns quietly if the resource is already gone.
    """
    for attempt in range(attempts):
        try:
            fn()
            return
        except Exception as exc:  # noqa: BLE001
            name = exc.__class__.__name__
            if name in _ALREADY_GONE:
                return
            if name not in _RETRYABLE or attempt == attempts - 1:
                raise
            time.sleep(delay)


def _runtime_exists(agentcore, runtime_id: str) -> bool:
    """Whether an AgentCore runtime is still present.

    DeleteAgentRuntime answers `AccessDeniedException` -- not `ResourceNotFound` --
    for a runtime that no longer exists, so the error alone cannot distinguish
    "already deleted" from "blocked by a dependency still tearing down". Checking
    first keeps a re-run from retrying for half a minute and then crying wolf.
    """
    try:
        runtimes = agentcore.list_agent_runtimes().get("agentRuntimes", [])
    except Exception:  # noqa: BLE001
        return True  # can't tell -- let the delete attempt speak for itself
    return any(r.get("agentRuntimeId") == runtime_id for r in runtimes)


def _delete_role(iam, role_name: str) -> None:
    """Detach a role's inline policies, then delete it."""
    for pol in iam.list_role_policies(RoleName=role_name).get("PolicyNames", []):
        iam.delete_role_policy(RoleName=role_name, PolicyName=pol)
    iam.delete_role(RoleName=role_name)


def _cleanup_agentcore_memory(rs: RunState, agentcore, failures: list[str]) -> None:
    """Delete the AgentCore memory store the `agentcore` CLI created for this run.

    The toolkit provisions this store itself, so it never lands in the run's
    `aws_resources` manifest and the manifest walk below cannot see it. Scoped to
    ids carrying this run id so no other run's memory is ever touched.
    """
    try:
        memories = agentcore.list_memories().get("memories", [])
    except Exception as exc:  # noqa: BLE001
        failures.append(f"AWS memory list: {exc.__class__.__name__}: {exc}")
        return

    for mem in memories:
        mid = mem.get("id") or ""
        if rs.run_id not in mid:
            continue
        try:
            _delete_with_retry(lambda mid=mid: agentcore.delete_memory(memoryId=mid))
        except Exception as exc:  # noqa: BLE001
            failures.append(f"AWS memory {mid}: {exc.__class__.__name__}: {exc}")


def _cleanup_aws(rs: RunState, failures: list[str]) -> None:
    try:
        session = aws_session()
    except Exception as exc:  # noqa: BLE001
        if rs.aws_resources:
            failures.append(f"AWS session unavailable, skipped AWS teardown: {exc}")
        return

    agentcore = session.client("bedrock-agentcore-control")
    lam = session.client("lambda")
    iam = session.client("iam")
    sm = session.client("secretsmanager")
    logs = session.client("logs")

    # Reverse order so dependents go before dependencies. The AgentCore deletes
    # additionally retry, because "before" is not enough on an async control plane.
    for res in reversed(rs.aws_resources):
        t, rid = res.get("type"), res.get("id")
        engine, gw = res.get("engine"), res.get("gateway")
        try:
            if t == "runtime":
                if _runtime_exists(agentcore, rid):
                    _delete_with_retry(
                        lambda i=rid: agentcore.delete_agent_runtime(agentRuntimeId=i)
                    )
            elif t == "policy":
                _delete_with_retry(
                    lambda i=rid, e=engine: agentcore.delete_policy(
                        policyEngineId=e, policyId=i
                    )
                )
            elif t == "policy_engine":
                _delete_with_retry(
                    lambda i=rid: agentcore.delete_policy_engine(policyEngineId=i)
                )
            elif t == "gateway_target":
                _delete_with_retry(
                    lambda i=rid, g=gw: agentcore.delete_gateway_target(
                        gatewayIdentifier=g, targetId=i
                    )
                )
            elif t == "gateway":
                _delete_with_retry(
                    lambda i=rid: agentcore.delete_gateway(gatewayIdentifier=i)
                )
            elif t == "lambda":
                _delete_with_retry(lambda i=rid: lam.delete_function(FunctionName=i))
                try:
                    logs.delete_log_group(logGroupName=f"/aws/lambda/{rid}")
                except Exception:  # noqa: BLE001
                    pass
            elif t == "iam_role":
                _delete_with_retry(lambda i=rid: _delete_role(iam, i))
            elif t == "secret":
                _delete_with_retry(
                    lambda i=rid: sm.delete_secret(
                        SecretId=i, ForceDeleteWithoutRecovery=True
                    )
                )
        except Exception as exc:  # noqa: BLE001
            failures.append(f"AWS {t} {rid}: {exc.__class__.__name__}: {exc}")

    _cleanup_agentcore_memory(rs, agentcore, failures)


def _cleanup_lakefs(rs: RunState, failures: list[str]) -> None:
    try:
        client, _ = lakefs_client_from_env()
    except Exception as exc:  # noqa: BLE001
        failures.append(f"lakeFS unavailable, skipped lakeFS teardown: {exc}")
        return

    scope = rs.scope
    # Close an unmerged PR (history is retained by lakeFS as audit record).
    if rs.pull_request_id and rs.pull_request_status not in {"merged", "closed"}:
        try:
            client.update_pull_request(rs.repository, rs.pull_request_id, status="closed")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"close PR {rs.pull_request_id}: {exc}")

    # Delete only the run's disposable branches. Never the source branch.
    for branch in (scope.workspace_branch, scope.baseline_branch):
        try:
            if client.branch_exists(rs.repository, branch):
                client.delete_branch(rs.repository, branch)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"delete branch {branch}: {exc}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    rs = RunState.load(args.run_id) if args.run_id else RunState.load_current()
    print(f"Cleaning up run {rs.run_id}...\n")

    failures: list[str] = []
    _cleanup_aws(rs, failures)
    _cleanup_lakefs(rs, failures)

    # Verify the source branch was never touched.
    try:
        client, _ = lakefs_client_from_env()
        if verify_source_unchanged(client, rs):
            print(f"Source branch {rs.source_branch!r} HEAD unchanged: {rs.source_commit}")
        else:
            failures.append("SOURCE BRANCH HEAD CHANGED -- investigate immediately")
    except Exception as exc:  # noqa: BLE001
        print(f"Could not verify source-branch integrity: {exc}")

    rs.stage = "cleaned"
    rs.save()

    if failures:
        print("\nCould not delete the following (delete manually if needed):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nCleanup complete. Repository and source branch preserved.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
