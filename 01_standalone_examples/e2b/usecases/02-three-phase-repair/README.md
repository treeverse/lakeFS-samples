# Use Case 2 — AI Data Agent: E2B + lakeFS + OpenAI

> Part of [**Agentic Data Patterns: E2B + lakeFS**](../../README.md). See the repo root for the full set of use cases.

> An AI agent repairs a broken dataset — iteratively, in isolation, with a full audit trail — and only promotes clean data to production.

---

## The problem this solves

When an AI agent writes data, three things can go wrong:

- **The generated code crashes** — and you have no record of what it tried.
- **The output passes local checks but fails downstream business rules** — and the bad data lands in production.
- **You can't reproduce or audit what the agent did** — so debugging or rolling back is manual.

This demo shows one way to solve all three using **E2B** for isolated execution and **lakeFS** as the control plane for AI-ready data.

---

## Demo story

A dirty e-commerce orders CSV is uploaded to a lakeFS repository. An AI agent — powered by OpenAI — is asked to repair it.

The agent works iteratively through **three progressive validation phases**:

| Phase | What is checked | Example failures in the sample data |
|-------|----------------|--------------------------------------|
| **1 — Structural Integrity** | Required fields present, no duplicate IDs, amount parseable | Missing `customer_id`, duplicate `ORD-001` |
| **2 — Format & Value Conformance** | Date format valid, amount > 0, currency = USD, enums lowercase | `SHIPPED` instead of `shipped`, date `01/17/2024`, amount `0.00`, currency `EUR` |
| **3 — Business Rules** | Order year ≥ 2023, amount ≤ $350 | `ORD-012` dated 2022, amount $420 |

On each attempt the agent:
1. Reads the current data from an isolated lakeFS branch
2. Is told **only the rules for the current failing phase** (it doesn't see Phase 2 or 3 rules until it earns them)
3. Asks OpenAI to generate Python repair code
4. Runs that code inside a disposable E2B sandbox
5. Validates the output and **commits the result to lakeFS** — pass or fail
6. Moves to the next phase once the current one passes

Only when all three phases pass does the branch merge into `main`, enforced by a **lakeFS pre-merge Action** that physically blocks any other promotion path.

The result is a lakeFS branch with 3–5 commits showing exactly how the agent progressed from broken to clean — each commit labelled with its phase and outcome.

---

## What you'll see

```
  Creating branch 'agent-run-20260529-201520' from 'main'...
  Branch created.
────────────────────────────────────────────────────────────
  Attempt 1/5
────────────────────────────────────────────────────────────
  Phase 1 (Structural Integrity): 2 failure(s)
    • order_id_unique: rows [6]
    • customer_id_required: rows [2]
  Generating repair code via OpenAI (gpt-4o-mini)...
  Running repair in E2B sandbox...
  Sandbox: i3n6e5kv3k3mkngkaxwox
  Result: phase 2 still failing — 10 rows, phase 2 (Format & Value Conformance) failed
  Committed: 2c90082ec01d
────────────────────────────────────────────────────────────
  Attempt 2/5
────────────────────────────────────────────────────────────
  Phase 2 (Format & Value Conformance): 5 failure(s)
    • order_date_format: rows [2, 5]
    • amount_positive: rows [2, 4]
    • currency_usd: rows [6]
    • status_valid: rows [3, 7]
    • product_category_valid: rows [4, 8]
  ...
  Committed: 4e33f0978fcc
────────────────────────────────────────────────────────────
  Attempt 4/5
────────────────────────────────────────────────────────────
  Phase 2 (Format & Value Conformance): 5 failure(s)
  ...
  Result: phase 3 still failing — 5 rows, phase 3 (Business Rules) failed
  Committed: d688bcfff4a1
────────────────────────────────────────────────────────────
  Attempt 5/5
────────────────────────────────────────────────────────────
  Phase 3 (Business Rules): 2 failure(s)
    • date_recency: rows [5]
    • amount_ceiling: rows [5]
  ...
  Result: PASSED — 4 rows, all phases passed
  Committed: 1cb832d99dc1

────────────────────────────────────────────────────────────
  Final Report
────────────────────────────────────────────────────────────
  Branch      : agent-run-20260529-201520
  Total attempts: 5
  Outcome     : PASSED

  Attempt 1: [FAIL]  commit=2c90082ec01d  phase 1 → phase 2 exposed
  Attempt 2: [FAIL]  commit=4e33f0978fcc  execution error
  Attempt 3: [FAIL]  commit=cb63e57263a6  execution error
  Attempt 4: [FAIL]  commit=d688bcfff4a1  phase 2 → phase 3 exposed
  Attempt 5: [PASS]  commit=1cb832d99dc1  4 rows, all phases passed

  Merging 'agent-run-20260529-201520' into 'main'...
  Merged successfully.
```

Every commit — including failed attempts and execution errors — is permanently recorded in lakeFS.

---

## Architecture

```
                        ┌─────────────────────┐
                        │   OpenAI API        │
                        │   code generation   │
                        └─────────┬───────────┘
                                  │ Python repair code (per phase)
                                  ▼
┌────────────────────────────────────────────────────────────┐
│                  Agent Loop (orchestrator)                  │
│                                                             │
│  validate → (phase 1?) → generate → E2B → validate         │
│                ↓ pass                                       │
│  validate → (phase 2?) → generate → E2B → validate         │
│                ↓ pass                                       │
│  validate → (phase 3?) → generate → E2B → validate         │
│                ↓ pass → commit → merge                      │
│                                                             │
│  Every attempt committed to lakeFS (pass or fail)          │
└────────┬──────────────────────────────┬────────────────────┘
         │                              │
         ▼                              ▼
┌─────────────────┐            ┌──────────────────────┐
│    lakeFS       │            │   E2B Cloud Sandbox  │
│                 │◄──────────►│                      │
│ • feature branch│  read CSV  │  /tmp/input.csv      │
│ • raw/          │  write CSV │  /tmp/repair_code.py │
│ • curated/      │            │  /tmp/output.csv     │
│ • validation/   │            │                      │
│ • code/         │            │  No access to your   │
│ • pre-merge gate│            │  network or infra    │
└────────┬────────┘            └──────────────────────┘
         │ merge only on phase 3 pass
         ▼
      main branch
```

---

## Why E2B and lakeFS together

| | E2B | lakeFS |
|---|---|---|
| **Isolation** | Each repair attempt runs in a fresh cloud VM — generated code cannot reach your network, credentials, or internal APIs | Each agent run gets its own data branch — intermediate and failed states never touch `main` |
| **Safety** | Sandbox is killed after every attempt, leaving no persistent state | Pre-merge Action physically blocks promotion unless the latest validation result is `"passed"` |
| **Auditability** | Stdout, stderr, exit code, and the generated code itself are captured per attempt | Every attempt — including failures and crashes — is a permanent, immutable lakeFS commit |
| **Reproducibility** | The exact repair code that produced the passing output is stored on the lakeFS branch | Any commit can be checked out and re-run against its exact input data |

---

## Sample dataset

`data/sample_bad_data.csv` contains 12 rows of intentionally broken e-commerce orders. The violations are spread across all three validation phases:

**Phase 1 — Structural** (obvious, agent sees these first):
- `ORD-002`: missing `customer_id`
- `ORD-001` appears twice (duplicate order ID)

**Phase 2 — Format & Values** (only visible after Phase 1 is clean):
- `ORD-003`: date in `MM/DD/YYYY` format, negative amount
- `ORD-004`: status `SHIPPED` (wrong case)
- `ORD-005`: amount `0.00`, category `Electronics` (wrong case)
- `ORD-007`: impossible date — month 13
- `ORD-008`: currency `EUR`
- `ORD-009`: status `unknown_status`
- `ORD-010`: category `CLOTHING` (wrong case)

**Phase 3 — Business Rules** (only visible after Phase 2 is clean):
- `ORD-012`: order dated 2022 (stale — year < 2023) and amount $420 (exceeds $350 threshold)

After all three phases pass, 4 valid rows remain.

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| **OpenAI API key** | [platform.openai.com](https://platform.openai.com) |
| **E2B API key** | sign up at [e2b.dev](https://e2b.dev), key from [e2b.dev/dashboard](https://e2b.dev/dashboard) |
| **lakeFS endpoint** | [lakeFS Cloud](https://lakefs.cloud/) or a self-hosted instance reachable from the internet |
| **lakeFS credentials** | Access key ID + secret access key |
| **lakeFS repository** | Create in the UI, or set `LAKEFS_STORAGE_NAMESPACE` to auto-create |
| **Python 3.11+** | `python --version` |
| **uv** | `pip install uv` or [docs.astral.sh/uv](https://docs.astral.sh/uv) |

---

## Quickstart

```bash
# 1. Clone lakeFS-samples and install the uv workspace (from this example's root)
git clone https://github.com/treeverse/lakeFS-samples
cd lakeFS-samples/01_standalone_examples/e2b
uv sync

# 2. Enter this use case (all commands below run from here)
cd usecases/02-three-phase-repair

# 3. Configure
cp .env.example .env
# Fill in OPENAI_API_KEY, E2B_API_KEY, LAKEFS_* in .env

# 4. Create the lakeFS repo and install the pre-merge gate
uv run python scripts/setup_lakefs_repo.py

# 5. Upload the sample broken dataset
uv run python scripts/upload_sample_data.py

# 6. Run the agent
uv run python -m agent_demo.main
```

Or run steps 4–6 in one command:
```bash
uv run python scripts/run_demo.py
```

---

## Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OPENAI_API_KEY` | Yes | — | OpenAI API key |
| `E2B_API_KEY` | Yes | — | E2B API key |
| `LAKEFS_ENDPOINT` | Yes | — | lakeFS server URL |
| `LAKEFS_ACCESS_KEY_ID` | Yes | — | lakeFS access key ID |
| `LAKEFS_SECRET_ACCESS_KEY` | Yes | — | lakeFS secret access key |
| `LAKEFS_REPOSITORY` | Yes | — | lakeFS repository name |
| `LAKEFS_STORAGE_NAMESPACE` | No | — | Storage path for auto-creating the repo (e.g. `s3://bucket/path/`) |
| `LAKEFS_BRANCH` | No | `main` | Source branch to read data from |
| `OPENAI_MODEL` | No | `gpt-4o-mini` | OpenAI model for code generation |
| `MAX_ATTEMPTS` | No | `5` | Maximum repair attempts |

---

## Inspecting results

**Objects written per agent run:**
```
raw/orders.csv                     source data (read-only, from main)
curated/repaired_orders.csv        latest repaired output
validation/latest_result.json      final validation state (read by pre-merge gate)
validation/attempt_N_result.json   per-attempt validation history
code/repair_attempt_N.py           the exact code OpenAI generated for each attempt
```

**In the lakeFS UI:** open the feature branch (`agent-run-YYYYMMDD-HHMMSS`) and browse the commit log. Each commit message names the phase and outcome: `Attempt 3 — phase 2 (Format & Value Conformance): failed`.

**Via lakectl:**
```bash
# Commit history for a run
lakectl log lakefs://data-agent-e2b-demo/agent-run-20260529-201520

# See exactly what changed vs main
lakectl diff lakefs://data-agent-e2b-demo/main lakefs://data-agent-e2b-demo/agent-run-20260529-201520

# Read the validation result
lakectl fs cat lakefs://data-agent-e2b-demo/agent-run-20260529-201520/validation/latest_result.json

# Read the repair code from attempt 3
lakectl fs cat lakefs://data-agent-e2b-demo/agent-run-20260529-201520/code/repair_attempt_3.py
```

**Test the pre-merge gate on a failed branch:**
```bash
# This will be blocked by the lakeFS Action
lakectl merge lakefs://data-agent-e2b-demo/agent-run-20260529-195356 lakefs://data-agent-e2b-demo/main
# Error: Pre-merge gate FAILED: latest validation did not pass.
```

---

## How the pre-merge gate works — and why it matters

`lakefs_actions/validate_data.yaml` defines a lakeFS Action that runs on every merge attempt into `main`. It is a Lua script that:

1. Reads `validation/latest_result.json` from the source branch
2. Checks whether `status == "passed"`
3. Blocks the merge with a descriptive error if not

The setup script uploads this file to `_lakefs_actions/validate_data.yaml` on the `main` branch, where lakeFS picks it up automatically.

**Isn't this redundant with the Python check?**

In this demo, the orchestrator already checks `if result.passed` before calling merge, so on the happy path the hook and the application code enforce the same thing. That's intentional — and worth explaining honestly.

The hook's value is not about this demo's code path. It's about every *other* merge path that the orchestrator doesn't control:

- `lakectl merge` run directly from a terminal
- A merge triggered from the lakeFS UI
- Another service or CI job calling the lakeFS API
- A modified version of this script that removes the check

In all of those cases, the Python-side check does nothing. The hook is the only thing that enforces the gate. It runs server-side, before the merge is applied, regardless of what client triggered it.

This distinction matters in real enterprise deployments where multiple teams, services, and tools have write access to the same repository. "The application checked before merging" is not sufficient for compliance or auditability — "the storage system enforced it at the infrastructure layer" is.

To see the hook block a real merge attempt, run:

```bash
# The first failed run left its branch un-merged — try to force-merge it
lakectl merge lakefs://data-agent-e2b-demo/agent-run-20260529-195356 lakefs://data-agent-e2b-demo/main
# Error: Pre-merge gate FAILED: latest validation did not pass.
```

---

## Mapping to real enterprise AI infrastructure

This pattern addresses a genuine problem in production AI platforms: **agents that write to data need the same guardrails as humans that write to data — isolation, validation, and controlled promotion.**

The same architecture applies to:
- **LLM fine-tuning pipelines** — agent-generated training data goes through quality gates before joining the training set
- **Feature engineering** — agent-computed features are validated against schema and distribution checks before landing in the feature store
- **Model evaluation datasets** — curated test sets are versioned and gated so evaluation results are reproducible

In each case: E2B provides compute isolation (the agent's code can't reach production systems), lakeFS provides data isolation (the agent's output can't reach production data until it passes), and the pre-merge gate provides the enforcement point.

---

## Related: lakeFS Mount (FUSE)

This demo passes data through the lakeFS Python SDK. **Use case 3 — [`../03-mount-receipts`](../03-mount-receipts)** shows the lakeFS **Mount** approach instead: the branch is exposed as a mounted filesystem inside the E2B sandbox, so the agent reads and writes with ordinary file I/O (`/mnt/repo/...`) — no SDK calls, no S3 knowledge — and versions each pass with `everest commit`. The isolation and pre-merge gate remain identical. E2B's full kernel access (including FUSE) makes this possible.

---

## Troubleshooting

**`ERROR: Required environment variable 'X' is not set`**
Your `.env` file is missing or incomplete. Run `cp .env.example .env` and fill in all required values.

**`ERROR: lakeFS repository '...' does not exist`**
Create the repository in the lakeFS UI, or set `LAKEFS_STORAGE_NAMESPACE` to a valid storage path to allow auto-creation.

**All attempts fail with `execution_error`**
The generated code crashed inside E2B. The stderr from the sandbox is included in `validation/attempt_N_result.json` on the branch, and is also fed back to the LLM on the next attempt. Switching to `OPENAI_MODEL=gpt-4o` improves code quality significantly.

**Stuck on the same phase after multiple attempts**
Inspect the generated code at `code/repair_attempt_N.py` on the feature branch. Common causes: the LLM doesn't know about a data edge case, or the validation rule description isn't specific enough. The validation failure description is the primary signal the LLM acts on.

**Pre-merge gate blocks a merge you want to force**
Edit or delete `_lakefs_actions/validate_data.yaml` on `main`. This is intentional — the gate is a guardrail, not a suggestion.
