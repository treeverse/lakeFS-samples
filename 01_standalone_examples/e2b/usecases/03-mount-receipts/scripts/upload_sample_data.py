"""Generate the messy receipt inbox and upload it to lakeFS main under inbox/."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from lakefs_e2b_common.lakefs_client import commit, get_or_create_repo, make_client, write_object
from mount_receipts.config import load_config
from mount_receipts.sample_data import generate

ORACLE = "_expected_manifest.json"


def main() -> None:
    cfg = load_config()
    client = make_client(cfg.lakefs_endpoint, cfg.lakefs_access_key_id, cfg.lakefs_secret_access_key)
    repo = get_or_create_repo(client, cfg.lakefs_repository, cfg.lakefs_storage_namespace)
    branch = repo.branch(cfg.lakefs_branch)

    # Clear any existing inbox so re-uploads are clean (no stale files from older formats).
    stale = [o.path for o in branch.objects(prefix="inbox/", max_amount=100000)]
    if stale:
        branch.delete_objects(stale)
        print(f"  Cleared {len(stale)} existing inbox object(s).")

    with tempfile.TemporaryDirectory() as tmp:
        summary = generate(tmp)
        n = 0
        for p in sorted(Path(tmp).iterdir()):
            if p.name == ORACLE:
                continue  # keep the oracle out of the agent's inbox
            write_object(branch, f"inbox/{p.name}", p.read_bytes())
            n += 1
        # store the oracle at the repo root for the demo author (agent never reads it)
        write_object(branch, "expected_manifest.json", json.dumps(summary, indent=2))

    print(f"  Uploaded {n} receipt files to inbox/ on '{cfg.lakefs_branch}'.")
    commit_id = commit(branch, "Upload messy receipt inbox", {"source": "upload_sample_data"})
    print(f"  Committed: {commit_id[:12]}")
    print(f"  Expected outcome: {summary['accept']} accepted, {summary['reject']} rejected, {summary['drop']} dropped.")


if __name__ == "__main__":
    main()
