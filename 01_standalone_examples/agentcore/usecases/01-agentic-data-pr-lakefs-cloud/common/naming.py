"""Deterministic run-scope naming -- the authorization boundary of the demo.

Given a run id, this module computes every branch name and object prefix the run
is permitted to touch. The Gateway Lambda derives its allow-lists exclusively
from these functions using server-controlled run state, so the model can never
widen its own scope by supplying a different branch or prefix.

Layout for a run::

    <repo>
      main                         (source branch -- NEVER written, HEAD verified)
      agentcore-demo/<run>/baseline    (disposable, seeded corpus, PR destination)
        agentcore-demo/<run>/corpus/   (synthetic input the agent reads)
      agentcore-demo/<run>/workspace   (disposable, agent writes here only)
        agentcore-demo/<run>/output/   (curated corpus, quarantine, reports)
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass

DEMO_NAMESPACE = "agentcore-demo"

# A run id is short, url/branch-safe, and unique per run.
_RUN_ID_RE = re.compile(r"^[a-z0-9]{8,12}$")


def new_run_id() -> str:
    """Generate a fresh, branch-safe run id."""
    return uuid.uuid4().hex[:10]


def validate_run_id(run_id: str) -> str:
    if not _RUN_ID_RE.match(run_id):
        raise ValueError(
            f"invalid run id {run_id!r}: expected 8-12 lowercase alphanumerics"
        )
    return run_id


@dataclass(frozen=True)
class RunScope:
    """The complete, immutable authorization scope for a single run.

    Constructed from a run id (and the selected source branch). Every field the
    Lambda uses to authorize a call comes from here -- not from tool arguments.
    """

    run_id: str
    source_branch: str = "main"

    @property
    def prefix(self) -> str:
        # Object-path namespace. Slashes are valid in lakeFS object keys and give
        # the corpus/output a folder structure.
        return f"{DEMO_NAMESPACE}/{self.run_id}"

    # --- branches --------------------------------------------------------
    # NOTE: lakeFS branch ids may contain only letters, digits, underscores and
    # dashes (no slashes). The spec's illustrative "agentcore-demo/<run>/baseline"
    # form is therefore rendered with dashes here, keeping the same components.
    @property
    def _branch_base(self) -> str:
        return f"{DEMO_NAMESPACE}-{self.run_id}"

    @property
    def baseline_branch(self) -> str:
        return f"{self._branch_base}-baseline"

    @property
    def workspace_branch(self) -> str:
        return f"{self._branch_base}-workspace"

    # --- object prefixes -------------------------------------------------
    @property
    def corpus_prefix(self) -> str:
        return f"{self.prefix}/corpus/"

    @property
    def output_prefix(self) -> str:
        return f"{self.prefix}/output/"

    @property
    def curated_us_prefix(self) -> str:
        return f"{self.output_prefix}curated/us/"

    @property
    def quarantine_manifest_path(self) -> str:
        return f"{self.output_prefix}quarantine/manifest.json"

    @property
    def report_json_path(self) -> str:
        return f"{self.output_prefix}reports/curation-report.json"

    @property
    def report_md_path(self) -> str:
        return f"{self.output_prefix}reports/curation-report.md"

    @property
    def validation_path(self) -> str:
        return f"{self.output_prefix}reports/validation-results.json"

    # --- read/write authorization helpers --------------------------------
    def readable_branches(self) -> set[str]:
        """Branches the agent may read from."""
        return {self.baseline_branch, self.workspace_branch}

    def writable_branch(self) -> str:
        """The only branch the agent may write to."""
        return self.workspace_branch

    def read_prefixes(self, branch: str) -> tuple[str, ...]:
        """Allowed read prefixes for a given branch."""
        if branch == self.baseline_branch:
            return (self.corpus_prefix,)
        if branch == self.workspace_branch:
            return (self.corpus_prefix, self.output_prefix)
        return ()

    def write_prefixes(self, branch: str) -> tuple[str, ...]:
        """Allowed write prefixes for a given branch."""
        if branch == self.writable_branch():
            return (self.output_prefix,)
        return ()

    def is_write_allowed(self, branch: str, path: str) -> bool:
        return branch == self.writable_branch() and any(
            path.startswith(p) for p in self.write_prefixes(branch)
        )

    def is_read_allowed(self, branch: str, path: str) -> bool:
        return branch in self.readable_branches() and any(
            path.startswith(p) for p in self.read_prefixes(branch)
        )

    def is_protected_branch(self, branch: str) -> bool:
        """Branches the agent must never write to or merge into via the agent identity."""
        return branch in {self.source_branch, "main", self.baseline_branch}
