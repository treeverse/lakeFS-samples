"""Run repair code in an E2B cloud sandbox."""
from __future__ import annotations

from dataclasses import dataclass

from e2b import Sandbox


@dataclass
class SandboxResult:
    success: bool
    sandbox_id: str
    output_csv: str | None
    stdout: str
    stderr: str
    exit_code: int
    error: str | None


def run_repair(input_csv: str, repair_code: str, e2b_api_key: str) -> SandboxResult:
    """Upload input CSV and repair code to an E2B sandbox, run it, and return the result."""
    sandbox = None
    try:
        sandbox = Sandbox.create(api_key=e2b_api_key)
        sandbox_id = sandbox.sandbox_id

        sandbox.files.write("/tmp/input.csv", input_csv)
        sandbox.files.write("/tmp/repair_code.py", repair_code)

        pip_result = sandbox.commands.run(
            "pip install -q pandas python-dateutil", timeout=120
        )

        run_result = sandbox.commands.run(
            "python /tmp/repair_code.py", timeout=60
        )

        stdout = (run_result.stdout or "") + (pip_result.stdout or "")
        stderr = (run_result.stderr or "") + (pip_result.stderr or "")
        exit_code = run_result.exit_code

        if exit_code != 0:
            return SandboxResult(
                success=False,
                sandbox_id=sandbox_id,
                output_csv=None,
                stdout=stdout,
                stderr=stderr,
                exit_code=exit_code,
                error=f"Repair script exited with code {exit_code}",
            )

        output_csv = sandbox.files.read("/tmp/output.csv", format="text")

        return SandboxResult(
            success=True,
            sandbox_id=sandbox_id,
            output_csv=output_csv,
            stdout=stdout,
            stderr=stderr,
            exit_code=exit_code,
            error=None,
        )

    except Exception as exc:
        sandbox_id = sandbox.sandbox_id if sandbox else "unknown"
        return SandboxResult(
            success=False,
            sandbox_id=sandbox_id,
            output_csv=None,
            stdout="",
            stderr="",
            exit_code=-1,
            error=str(exc),
        )
    finally:
        if sandbox is not None:
            try:
                sandbox.kill()
            except Exception:
                pass
