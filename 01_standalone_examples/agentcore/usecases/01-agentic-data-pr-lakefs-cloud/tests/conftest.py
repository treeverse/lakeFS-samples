"""Shared test fixtures: an in-memory fake lakeFS client and a seeded run.

The fake models exactly the operations the sample uses (branches with copied
snapshots, objects, commits, diff, and pull requests that merge by advancing the
destination head). It lets the whole flow -- seed, curate, validate, PR, approve,
integrity check -- run in unit tests with no network.
"""

from __future__ import annotations

import itertools

import pytest

from gateway.lakefs_client import LakeFSError, ObjectStats

_COMMIT_SEQ = itertools.count(1)
_PR_SEQ = itertools.count(1)


class FakeLakeFSClient:
    def __init__(self, repositories=("demo-repo",), source_branch="main"):
        self._repos = list(repositories)
        # branch -> {path: (bytes, content_type)}
        self._branches: dict[str, dict[str, tuple[bytes, str]]] = {}
        self._heads: dict[str, str] = {}
        self._prs: dict[str, dict] = {}
        self.deleted_branches: list[str] = []
        if repositories:
            self._branches[source_branch] = {}
            self._heads[source_branch] = f"commit-{next(_COMMIT_SEQ)}"

    # --- repos ----------------------------------------------------------
    def list_repositories(self, amount=100):
        return [{"id": r} for r in self._repos]

    def get_repository(self, repository):
        if repository not in self._repos:
            raise LakeFSError("not found", status_code=404)
        return {"id": repository}

    def whoami(self):
        return {"user": "demo"}

    # --- branches -------------------------------------------------------
    def get_branch(self, repository, branch):
        if branch not in self._heads:
            raise LakeFSError("no branch", status_code=404)
        return {"id": branch, "commit_id": self._heads[branch]}

    def branch_head(self, repository, branch):
        return self.get_branch(repository, branch)["commit_id"]

    def branch_exists(self, repository, branch):
        return branch in self._heads

    def create_branch(self, repository, name, source, hidden=False):
        # source may be a branch name or a commit id; copy from a branch snapshot.
        src_objs = self._branches.get(source, {})
        if source not in self._branches:
            # find branch whose head == source commit
            for b, h in self._heads.items():
                if h == source:
                    src_objs = self._branches[b]
                    break
        self._branches[name] = dict(src_objs)
        self._heads[name] = f"commit-{next(_COMMIT_SEQ)}"
        return name

    def delete_branch(self, repository, branch):
        self._branches.pop(branch, None)
        self._heads.pop(branch, None)
        self.deleted_branches.append(branch)

    # --- objects --------------------------------------------------------
    def upload_object(self, repository, branch, path, content, content_type="application/octet-stream"):
        if isinstance(content, str):
            content = content.encode("utf-8")
        self._branches.setdefault(branch, {})[path] = (content, content_type)
        return ObjectStats(path=path, size_bytes=len(content), checksum=f"sha:{len(content)}", content_type=content_type)

    def get_object(self, repository, ref, path):
        objs = self._resolve_ref(ref)
        if path not in objs:
            raise LakeFSError("no object", status_code=404)
        return objs[path][0]

    def stat_object(self, repository, ref, path):
        objs = self._resolve_ref(ref)
        if path not in objs:
            raise LakeFSError("no object", status_code=404)
        c, ct = objs[path]
        return ObjectStats(path=path, size_bytes=len(c), checksum=f"sha:{len(c)}", content_type=ct)

    def _resolve_ref(self, ref):
        if ref in self._branches:
            return self._branches[ref]
        for b, h in self._heads.items():
            if h == ref:
                return self._branches[b]
        return {}

    def list_objects(self, repository, ref, prefix="", after="", amount=100, delimiter=""):
        objs = self._resolve_ref(ref)
        paths = sorted(p for p in objs if p.startswith(prefix) and p > after)
        page = paths[:amount]
        results = []
        for p in page:
            c, ct = objs[p]
            results.append({"path": p, "path_type": "object", "size_bytes": len(c), "checksum": f"sha:{len(c)}", "content_type": ct})
        return {"results": results, "pagination": {"has_more": len(paths) > amount, "next_offset": page[-1] if page else ""}}

    # --- commit / diff --------------------------------------------------
    def commit(self, repository, branch, message, metadata=None):
        cid = f"commit-{next(_COMMIT_SEQ)}"
        self._heads[branch] = cid
        return {"id": cid, "message": message, "metadata": metadata or {}}

    def diff_refs(self, repository, left_ref, right_ref, prefix="", amount=1000):
        left = self._resolve_ref(left_ref)
        right = self._resolve_ref(right_ref)
        out = []
        for p in sorted(right):
            if p not in left:
                out.append({"type": "added", "path": p})
            elif right[p] != left[p]:
                out.append({"type": "changed", "path": p})
        for p in sorted(left):
            if p not in right:
                out.append({"type": "removed", "path": p})
        return out

    # --- pull requests --------------------------------------------------
    def create_pull_request(self, repository, title, description, source_branch, destination_branch):
        pid = f"pr-{next(_PR_SEQ)}"
        self._prs[pid] = {
            "id": pid, "title": title, "description": description, "status": "open",
            "source_branch": source_branch, "destination_branch": destination_branch,
        }
        return dict(self._prs[pid])

    def get_pull_request(self, repository, pull_id):
        if str(pull_id) not in self._prs:
            raise LakeFSError("no pr", status_code=404)
        return dict(self._prs[str(pull_id)])

    def list_pull_requests(self, repository, status=None, amount=100):
        return [dict(p) for p in self._prs.values() if not status or p["status"] == status]

    def update_pull_request(self, repository, pull_id, status=None, title=None, description=None):
        pr = self._prs[str(pull_id)]
        if status:
            pr["status"] = status

    def merge_pull_request(self, repository, pull_id):
        pr = self._prs[str(pull_id)]
        src, dst = pr["source_branch"], pr["destination_branch"]
        # Apply source objects onto destination and advance destination head.
        self._branches[dst].update(self._branches[src])
        self._heads[dst] = f"commit-{next(_COMMIT_SEQ)}"
        pr["status"] = "merged"
        pr["merged_commit_id"] = self._heads[dst]
        return {"reference": self._heads[dst]}

    def pull_request_web_url(self, repository, pull_id):
        return f"https://lakefs.example/repositories/{repository}/pulls/{pull_id}"


@pytest.fixture
def fake_client():
    return FakeLakeFSClient()


@pytest.fixture
def tmp_generated(tmp_path, monkeypatch):
    import common.runstate as runstate

    gen = tmp_path / "generated"
    gen.mkdir()
    monkeypatch.setattr(runstate, "GENERATED_DIR", gen)
    monkeypatch.setattr(runstate, "CURRENT_POINTER", gen / "current-run.json")
    return gen


@pytest.fixture
def seeded_run(fake_client, tmp_generated):
    from orchestrator.seeding import seed_run

    return seed_run(fake_client, "demo-repo", "main")


def patch_client(monkeypatch, module_dotted_names, fake_client, cfg=None):
    """Patch ``lakefs_client_from_env`` in the given already-imported modules."""
    import importlib

    from common.config import LakeFSConfig, _Secret

    cfg = cfg or LakeFSConfig(
        endpoint="https://lakefs.example", access_key_id="AKIA", _secret=_Secret("s"),
        repository="demo-repo", source_branch="main",
    )
    for name in module_dotted_names:
        mod = importlib.import_module(name)
        monkeypatch.setattr(mod, "lakefs_client_from_env", lambda: (fake_client, cfg))
