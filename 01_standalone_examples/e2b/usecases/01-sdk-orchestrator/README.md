# Use Case 1 — SDK Orchestrator

> Part of [**Agentic Data Patterns: E2B + lakeFS**](../../README.md). See the repo root for the full set of use cases.

A minimal Python POC demonstrating the **Agentic Data PR** pattern:

> **E2B** provides isolated compute for agent-generated code.  
> **lakeFS** provides isolated, versioned data branches for that code's side-effects.  
>
> The agent runs inside an E2B cloud sandbox, reads raw data from a lakeFS branch,
> writes transformed outputs back to that same branch, and then — only if validation
> passes — the **host** (your machine) commits and merges the branch into `main`.
> Failed agent runs leave `main` untouched.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   HOST  (your machine)                      │
│                                                             │
│   orchestrator.py                                           │
│   ┌──────────────────────┐   ┌──────────────────────────┐  │
│   │   lakeFS SDK         │   │   E2B SDK                │  │
│   │                      │   │                          │  │
│   │ 1. create branch     │   │ 4. start sandbox         │  │
│   │ 2. (wait for agent)  │   │ 5. upload agent_job.py   │  │
│   │ 3. list uncommitted  │   │ 6. pip install deps      │  │
│   │ 7. commit branch     │   │    run agent_job.py      │  │
│   │ 8. merge if pass     │   │                          │  │
│   └──────────┬───────────┘   └─────────────┬────────────┘  │
│              │                             │               │
└──────────────┼─────────────────────────────┼───────────────┘
               │                             │
               ▼                             ▼
      ┌────────────────┐           ┌──────────────────────┐
      │    lakeFS      │◄──boto3───│  E2B Cloud Sandbox   │
      │                │   S3 GW   │                      │
      │ • branches     │           │  agent_job.py        │
      │ • commits      │           │  • read  raw/orders  │
      │ • objects      │           │  • clean & transform │
      └────────────────┘           │  • write curated/    │
                                   │  • write reports/    │
                                   │  • print JSON result │
                                   └──────────────────────┘
```

### Key isolation properties

| Dimension | Isolation mechanism |
|-----------|---------------------|
| Compute   | E2B cloud sandbox — ephemeral, network-isolated from your machine |
| Data      | lakeFS feature branch — zero-copy, completely isolated from `main` |
| Secrets   | Agent sees only the env vars it needs; host never logs secret values |
| Safety    | Only the host can commit/merge; agent only reads/writes objects |

---

## Why localhost lakeFS does not work from E2B

E2B sandboxes run in E2B's cloud infrastructure.  
A lakeFS endpoint on `localhost`, `127.0.0.1`, or `host.docker.internal` is your local
machine, not accessible from the cloud.

**Options:**
1. **lakeFS Cloud** — the easiest path: sign up at [lakefs.cloud](https://lakefs.cloud/)
2. **Tunnel** — expose your local lakeFS via [ngrok](https://ngrok.com),
   [Tailscale Funnel](https://tailscale.com/kb/1223/funnel/), or similar,
   then set `LAKEFS_ENDPOINT` to the tunnel URL
3. **Override** — set `ALLOW_LOCALHOST_LAKEFS=true` if your network is configured
   to make it work (rare; mostly useful for CI environments)

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| **E2B API key** | sign up at [e2b.dev](https://e2b.dev), key from [e2b.dev/dashboard](https://e2b.dev/dashboard) |
| **lakeFS endpoint** | [lakeFS Cloud](https://lakefs.cloud/) or a reachable self-hosted instance |
| **lakeFS credentials** | Access key ID + secret access key |
| **lakeFS repository** | Create it in the UI, or set `LAKEFS_STORAGE_NAMESPACE` to auto-create |
| **Python 3.11+** | `python --version` |
| **uv** | `pip install uv` or see [docs.astral.sh/uv](https://docs.astral.sh/uv) |

---

## Setup

```bash
# 1. Clone lakeFS-samples and install the uv workspace (from this example's root)
git clone https://github.com/treeverse/lakeFS-samples
cd lakeFS-samples/01_standalone_examples/e2b
uv sync

# 2. Enter this use case
cd usecases/01-sdk-orchestrator

# 3. Copy and fill in the environment file
cp .env.example .env
# edit .env with your real values

# 4. Run tests (no live credentials needed)
uv run pytest
```

> All commands below are run from `usecases/01-sdk-orchestrator/`.

---

## Demo script

### Step 1 — Seed demo data

```bash
uv run python -m lakefs_e2b_poc.seed
```

Uploads `raw/orders.csv` to the `main` branch and commits it.  
The CSV has intentionally dirty rows (negative amounts, missing IDs, non-USD currencies)
to exercise the agent's cleaning logic.

**Expected output:**
```
Connecting to lakeFS at https://your-org.lakefscloud.io ...
Repository 'orders-demo' is ready.
Branch 'main' exists.
Uploading s3://orders-demo/main/raw/orders.csv ...
Upload complete.
Committing 1 staged change(s) ...
Committed: ...
Seed complete.
  lakeFS repo : orders-demo
  branch      : main
  object      : raw/orders.csv  (232 bytes, 6 data rows)
```

---

### Step 2 — Pass scenario (agent succeeds → merge)

```bash
uv run python -m lakefs_e2b_poc.orchestrator --scenario pass --merge
```

Flow:
1. Creates `agent-run-YYYYMMDD-HHMMSS-pass` from `main`
2. Starts an E2B sandbox, uploads `agent_job.py`
3. Agent reads `raw/orders.csv`, cleans it (2 valid rows survive), writes:
   - `curated/orders.parquet`
   - `reports/data_quality.json`
   - `reports/agent_summary.md`
4. Agent prints `{"status":"pass","rows_in":6,"rows_out":2,...}`
5. Host commits the branch and merges it into `main`

**Expected output:**
```
────────────────────────────────────────────────────────────
  lakeFS: create branch
────────────────────────────────────────────────────────────
  Connecting to https://your-org.lakefscloud.io ...
  Creating 'agent-run-20260515-120000-pass' from 'main' ...
  Branch created.

────────────────────────────────────────────────────────────
  E2B: start sandbox
────────────────────────────────────────────────────────────
  Sandbox ID : sbx_abc123

  ...

────────────────────────────────────────────────────────────
  lakeFS: merge into main
────────────────────────────────────────────────────────────
  Merged 'agent-run-20260515-120000-pass' → 'main'

────────────────────────────────────────────────────────────
  Done
────────────────────────────────────────────────────────────
  status   : pass
  sandbox  : sbx_abc123
  branch   : agent-run-20260515-120000-pass
```

---

### Step 3 — Fail scenario (agent fails → no merge)

```bash
uv run python -m lakefs_e2b_poc.orchestrator --scenario fail
```

The agent applies an intentionally strict filter (`amount > 1000`) that produces
0 valid rows.  It writes failure reports to the branch but the host sees
`"status":"fail"` and refuses to merge.

**Expected output:**
```
  ...
────────────────────────────────────────────────────────────
  Result: FAILED — branch NOT merged
────────────────────────────────────────────────────────────
  Reason  : Validation failed: expected > 0 output rows but got 0 ...
  Branch  : agent-run-20260515-120005-fail
  Inspect : uv run python -m lakefs_e2b_poc.inspect --ref agent-run-20260515-120005-fail
```

---

### Step 4 — Inspect the result

```bash
# Inspect main (should now contain the curated data from the pass run)
uv run python -m lakefs_e2b_poc.inspect --ref main

# Inspect the failed branch
uv run python -m lakefs_e2b_poc.inspect --ref agent-run-YYYYMMDD-HHMMSS-fail
```

**Expected output:**
```
Repo  : orders-demo
Ref   : main
Host  : https://your-org.lakefscloud.io

────────────────────────────────────────────────────────────
Objects
────────────────────────────────────────────────────────────
  curated/orders.parquet                               8,192 bytes
  raw/orders.csv                                         232 bytes
  reports/agent_summary.md                               512 bytes
  reports/data_quality.json                              256 bytes

────────────────────────────────────────────────────────────
Recent commits
────────────────────────────────────────────────────────────
  a1b2c3d4e5f6  'Merge agent-run-20260515-120000-pass into main'
  ...
```

---

## What to screenshot for a blog post

1. **Terminal output** — E2B sandbox ID appearing, lakeFS branch name, final status line
2. **lakeFS branch diff** — the feature branch showing the 3 new objects vs. `main`
3. **lakeFS commit history** — `main` with the merge commit from the pass run
4. **Failed branch** — the `fail` branch present in the UI with its reports, clearly not merged

---

## API notes (verified against live docs)

| Item | Behaviour observed |
|------|--------------------|
| `Sandbox.create(envs={})` | Env vars set at sandbox-level, available to all commands |
| `sandbox.commands.run(cmd, timeout=N)` | Default timeout 60 s; override for long operations |
| `sandbox.files.write(path, data)` | Accepts `str`, `bytes`, or file-like `IO` |
| `branch.log(max_amount=N)` | Returns iterable of `Commit` objects with `.id`, `.message`, `.committer` |
| `feature.merge_into(main)` | Merges feature branch into main; returns merge result |
| lakeFS S3 key format | `<bucket>=<repo>`, `<key>=<branch>/<path>` |
| `botocore.Config` checksum flags | `request_checksum_calculation="when_required"` avoids HTTP 501 errors |

---

## Future extensions

- **LLM-generated transforms** — replace the deterministic `agent_job.py` with
  code generated by Claude / GPT at runtime, then executed in the E2B sandbox
- **Custom E2B template** — pre-install pandas/pyarrow/boto3 into a custom image
  to skip the pip-install step and cut sandbox startup time
- **lakeFS hooks** — add pre-merge hooks that run additional validation before
  the orchestrator is allowed to merge
- **Branch protection** — configure lakeFS branch protection rules on `main` so
  only commits that pass quality checks can land
- **Human approval** — add a human-in-the-loop step between "agent wrote data"
  and "orchestrator merges"; show a diff in the terminal and ask for confirmation
- **Credential scoping** — where lakeFS supports it, scope the agent's credentials
  to read-only on `main` and write-only on its own branch
