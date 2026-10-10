"""Per-run state: the server-controlled record of everything a run created.

Persisted to ``generated/<run-id>.json`` with a ``generated/current-run.json``
pointer to the most recent run. The Gateway Lambda reads a copy of the run scope
(baked into its environment / the demo-context response) so that authorization is
always derived from server state, never from model input.

The ``aws_resources`` and ``lakefs_resources`` sections are the manifest that
``make cleanup`` walks to tear a run down safely and scoped to the run namespace.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import SAMPLE_ROOT
from .naming import RunScope, new_run_id, validate_run_id

GENERATED_DIR = SAMPLE_ROOT / "generated"
CURRENT_POINTER = GENERATED_DIR / "current-run.json"


def utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class RunState:
    run_id: str
    repository: str
    source_branch: str = "main"

    # Recorded HEADs / commits (immutability evidence).
    source_commit: str | None = None
    baseline_commit: str | None = None
    workspace_commit: str | None = None  # the validated commit the PR points at

    # AgentCore execution identifiers.
    agentcore_session_id: str | None = None
    agentcore_trace_id: str | None = None
    gateway_id: str | None = None
    gateway_url: str | None = None
    runtime_arn: str | None = None

    # Pull request.
    pull_request_id: str | None = None
    pull_request_status: str | None = None

    # Progress.
    stage: str = "created"
    validation_status: str = "unknown"

    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)

    # Cleanup manifests. Lists of {"type","id","region",...} records.
    aws_resources: list[dict[str, Any]] = field(default_factory=list)
    lakefs_resources: list[dict[str, Any]] = field(default_factory=list)

    # --- scope -----------------------------------------------------------
    @property
    def scope(self) -> RunScope:
        return RunScope(run_id=self.run_id, source_branch=self.source_branch)

    # --- factory ---------------------------------------------------------
    @classmethod
    def create(cls, repository: str, source_branch: str = "main") -> RunState:
        return cls(
            run_id=new_run_id(),
            repository=repository,
            source_branch=source_branch,
        )

    # --- resource tracking ----------------------------------------------
    def record_aws_resource(self, **record: Any) -> None:
        self.aws_resources.append(record)
        self.touch()

    def record_lakefs_resource(self, **record: Any) -> None:
        self.lakefs_resources.append(record)
        self.touch()

    def touch(self) -> None:
        self.updated_at = utc_now_iso()

    # --- persistence -----------------------------------------------------
    def path(self) -> Path:
        return GENERATED_DIR / f"{self.run_id}.json"

    def save(self) -> Path:
        GENERATED_DIR.mkdir(parents=True, exist_ok=True)
        self.touch()
        p = self.path()
        p.write_text(json.dumps(asdict(self), indent=2, sort_keys=True))
        CURRENT_POINTER.write_text(json.dumps({"run_id": self.run_id}, indent=2))
        return p

    @classmethod
    def load(cls, run_id: str) -> RunState:
        validate_run_id(run_id)
        p = GENERATED_DIR / f"{run_id}.json"
        if not p.exists():
            raise FileNotFoundError(f"no run state for run id {run_id!r} at {p}")
        return cls(**json.loads(p.read_text()))

    @classmethod
    def load_current(cls) -> RunState:
        if not CURRENT_POINTER.exists():
            raise FileNotFoundError(
                "no current run. Run `make seed` (or `make run`) to start one."
            )
        run_id = json.loads(CURRENT_POINTER.read_text())["run_id"]
        return cls.load(run_id)

    def demo_context(self, allowed_input_prefix: str | None = None) -> dict[str, Any]:
        """The server-defined context handed to the agent via get_demo_context.

        Contains only scope -- no credentials, no secret ARNs.
        """
        s = self.scope
        return {
            "run_id": self.run_id,
            "repository": self.repository,
            "source_branch": self.source_branch,
            "source_commit": self.source_commit,
            "baseline_branch": s.baseline_branch,
            "baseline_commit": self.baseline_commit,
            "workspace_branch": s.workspace_branch,
            "allowed_input_prefix": allowed_input_prefix or s.corpus_prefix,
            "allowed_output_prefix": s.output_prefix,
        }
