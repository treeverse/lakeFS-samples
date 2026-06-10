"""Main iterative repair loop: validate → generate → run in E2B → commit → repeat."""
from __future__ import annotations

import json
from dataclasses import dataclass

import lakefs

from agent_demo.config import Config
from agent_demo.e2b_runner import run_repair
from lakefs_e2b_common.lakefs_client import commit, read_object, write_object
from agent_demo.openai_agent import AttemptContext, generate_repair_code
from agent_demo.validation import PHASE_NAMES, ValidationFailure, ValidationResult, validate


@dataclass
class AgentRunResult:
    branch_name: str
    passed: bool
    total_attempts: int
    attempts: list[dict]
    final_commit_id: str | None
    final_validation: ValidationResult | None


def _divider() -> None:
    print("─" * 60)


def run(
    config: Config,
    lakefs_client_obj,
    repo: lakefs.Repository,
    branch_name: str,
) -> AgentRunResult:
    """Run the progressive repair loop, advancing through validation phases until all pass."""
    branch = repo.branch(branch_name)
    current_csv = read_object(branch, "raw/orders.csv")

    previous_attempts: list[AttemptContext] = []
    attempt_summaries: list[dict] = []
    final_commit_id: str | None = None
    final_validation: ValidationResult | None = None
    passed = False

    for attempt in range(1, config.max_attempts + 1):
        _divider()
        print(f"  Attempt {attempt}/{config.max_attempts}")
        _divider()

        validation = validate(current_csv)

        if validation.passed:
            passed = True
            final_validation = validation
            print(f"  Validation: PASSED — {validation.summary}")
            break

        phase = validation.failed_phase
        phase_name = PHASE_NAMES[phase]
        print(f"  Phase {phase} ({phase_name}): {len(validation.failures)} failure(s)")
        for f in validation.failures:
            print(f"    • {f.rule}: rows {f.affected_rows}")

        print(f"  Generating repair code via OpenAI ({config.openai_model})...")
        repair_code = generate_repair_code(
            openai_api_key=config.openai_api_key,
            model=config.openai_model,
            current_csv=current_csv,
            current_phase=phase,
            validation_failures=validation.failures,
            previous_attempts=previous_attempts,
        )

        print(f"  Running repair in E2B sandbox...")
        sandbox_result = run_repair(current_csv, repair_code, config.e2b_api_key)
        print(f"  Sandbox: {sandbox_result.sandbox_id}")

        if sandbox_result.success and sandbox_result.output_csv:
            repair_validation = validate(sandbox_result.output_csv)
            repaired_csv: str | None = sandbox_result.output_csv
        else:
            error_parts = [sandbox_result.error or "sandbox execution failed"]
            if sandbox_result.stderr.strip():
                error_parts.append(
                    f"Command exited with code {sandbox_result.exit_code} and error:\n{sandbox_result.stderr.strip()}"
                )
            repair_validation = ValidationResult(
                passed=False,
                failures=[ValidationFailure("execution_error", "\n".join(error_parts), [])],
                row_count=0,
                failed_phase=phase,
            )
            repaired_csv = None

        status_str = "passed" if repair_validation.passed else "failed"
        result_phase = repair_validation.failed_phase
        print(
            f"  Result: {'PASSED' if repair_validation.passed else f'phase {result_phase} still failing'}"
            f" — {repair_validation.summary}"
        )

        if repaired_csv:
            write_object(branch, "curated/repaired_orders.csv", repaired_csv)

        validation_json = json.dumps(repair_validation.to_dict(), indent=2)
        write_object(branch, "validation/latest_result.json", validation_json)
        write_object(branch, f"validation/attempt_{attempt}_result.json", validation_json)
        write_object(branch, f"code/repair_attempt_{attempt}.py", repair_code)

        metadata: dict[str, str] = {
            "attempt": str(attempt),
            "target_phase": str(phase),
            "status": status_str,
            "sandbox_id": sandbox_result.sandbox_id,
            "model": config.openai_model,
            "validation_summary": repair_validation.summary,
        }
        if config.e2b_team and sandbox_result.sandbox_id != "unknown":
            sandbox_url = (
                f"https://e2b.dev/dashboard/{config.e2b_team}"
                f"/sandboxes/{sandbox_result.sandbox_id}/monitoring"
            )
            metadata["::lakefs::E2B Sandbox::url[url:ui]"] = sandbox_url

        commit_id = commit(branch, f"Attempt {attempt} — phase {phase} ({PHASE_NAMES[phase]}): {status_str}", metadata)
        print(f"  Committed: {commit_id[:12]}")

        attempt_summaries.append({
            "attempt": attempt,
            "target_phase": phase,
            "phase_name": PHASE_NAMES[phase],
            "status": status_str,
            "commit_id": commit_id,
            "sandbox_id": sandbox_result.sandbox_id,
            "validation_summary": repair_validation.summary,
        })

        previous_attempts.append(AttemptContext(
            attempt=attempt,
            repair_code=repair_code,
            validation_result=repair_validation,
            e2b_stdout=sandbox_result.stdout,
            e2b_stderr=sandbox_result.stderr,
            e2b_exit_code=sandbox_result.exit_code,
        ))
        final_commit_id = commit_id
        final_validation = repair_validation

        if repair_validation.passed:
            passed = True
            break

        if repaired_csv:
            current_csv = repaired_csv

    return AgentRunResult(
        branch_name=branch_name,
        passed=passed,
        total_attempts=len(attempt_summaries),
        attempts=attempt_summaries,
        final_commit_id=final_commit_id,
        final_validation=final_validation,
    )
