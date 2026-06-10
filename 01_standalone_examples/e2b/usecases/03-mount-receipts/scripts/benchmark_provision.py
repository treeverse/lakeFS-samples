"""Benchmark per-run sandbox setup: prebaked template vs runtime install.

The template's whole job is to remove per-run provisioning (apt fuse3 + uploading the
everest binary + pip deps). This measures exactly that — sandbox create + provision
(i.e. time until the sandbox is ready to mount) — isolated from the vision/agent work,
so the numbers are dataset-independent and honest.

    uv run python scripts/benchmark_provision.py --mode template --runs 3
    uv run python scripts/benchmark_provision.py --mode runtime  --runs 3
"""
from __future__ import annotations

import argparse
import json
import statistics
import time

from e2b import Sandbox

from mount_receipts import e2b_session as sess
from mount_receipts.config import load_config


def one_run(cfg, use_template: bool) -> dict:
    kw = {"api_key": cfg.e2b_api_key, "envs": cfg.sandbox_envs(), "timeout": 900}
    if use_template:
        kw["template"] = cfg.e2b_template
    t0 = time.monotonic()
    sbx = Sandbox.create(**kw)
    t1 = time.monotonic()
    try:
        sess.provision(sbx, use_template=use_template)
        t2 = time.monotonic()
    finally:
        try:
            sbx.kill()
        except Exception:
            pass
    return {"create_s": round(t1 - t0, 1), "provision_s": round(t2 - t1, 1), "ready_s": round(t2 - t0, 1)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["template", "runtime"], required=True)
    ap.add_argument("--runs", type=int, default=3)
    args = ap.parse_args()

    cfg = load_config()
    use_template = args.mode == "template"
    if use_template and not cfg.e2b_template:
        raise SystemExit("E2B_TEMPLATE is not set — can't benchmark the template path.")

    rows = []
    for i in range(args.runs):
        r = one_run(cfg, use_template)
        print(f"  run {i + 1}/{args.runs}: create={r['create_s']}s  provision={r['provision_s']}s  ready={r['ready_s']}s")
        rows.append(r)

    agg = {
        k: {
            "mean": round(statistics.mean(r[k] for r in rows), 1),
            "min": min(r[k] for r in rows),
            "max": max(r[k] for r in rows),
        }
        for k in ("create_s", "provision_s", "ready_s")
    }
    print(json.dumps({"mode": args.mode, "template": cfg.e2b_template or None, "runs": args.runs, "agg": agg}, indent=2))


if __name__ == "__main__":
    main()
