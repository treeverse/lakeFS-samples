# Use Case 3 — Receipts → Clean Ledger (lakeFS Mount)

> Part of [**Agentic Data Patterns: E2B + lakeFS**](../../README.md). See the repo root for the full set of use cases.

> An AI agent turns a messy dump of receipt & invoice **images and PDFs** into a
> validated ledger — iteratively, in an E2B sandbox, working directly on a **lakeFS
> branch mounted as a filesystem** — and only a clean ledger is allowed to merge to `main`.

---

## What makes this one different

Use cases 1 and 2 pass data through the lakeFS **Python SDK** (read object → process →
write object → commit). This one uses **lakeFS Mount (`everest`)**, which exposes a branch
as a real filesystem path.

That matters because **most of what agents actually do happens on a filesystem.** The agent
here never imports the lakeFS SDK and never knows about S3 — it does plain file I/O on
`/home/user/mnt/...`:

```python
for path in glob("/home/user/mnt/inbox/*"):   # ordinary filesystem
    png = load_image_png(path)                 # ordinary file read (PDF → raster)
    fields = vision_extract(png)               # gpt-4o multimodal
    append_row("/home/user/mnt/ledger.csv", fields)   # ordinary file write
```

Versioning happens out-of-band: after each phase the host runs `everest commit`, which
snapshots the mounted changes back to the lakeFS branch (via a temporary `everest-w-*`
write branch that is merged into the source branch).

---

## Demo story

A chaotic `inbox/` is uploaded to a lakeFS repo — receipts across **many formats**
(`jpg`, `png`, `webp`, `bmp`, `tiff`, single- and multi-page `pdf`), plus duplicates, a
corrupt file, a non-receipt photo, an unsupported `.txt`, and several invalid invoices. The
agent works in **three progressive phases**, each committed to its branch:

| Phase | What it does | What it catches in the sample set |
|-------|--------------|------------------------------------|
| **1 — Triage (structural)** | hash-dedupe, drop corrupt/unreadable & unsupported files, drop non-receipts (vision `is_receipt`) | a corrupt file, an exact duplicate, a beach photo, a `.txt` note |
| **2 — Extract (multimodal)** | gpt-4o reads each kept file → `{vendor, date, invoice_no, currency, line_items, total}` cached as sidecars → `ledger_draft` (multi-page PDFs are page-stacked so a total on page 2 is read) | (the low-contrast receipt still extracts) |
| **3 — Validate (business rules)** | the agent **writes its own Python validator at runtime** for the per-receipt rules and runs it in the sandbox: `total == sum(items)`, date sane & not future, currency = USD, total ≤ \$500 cap — and now a 7th rule: flag a row **`ambiguous`** instead of rejecting it outright when it has exactly one recoverable problem. The host then applies the cross-row **unique invoice no** rule deterministically. | math mismatch, future-dated, a confidently-read non-USD currency, over-cap, duplicate invoice # (hard rejects); a receipt with **no legible currency indicator** and a smudged-scan total mismatch (**ambiguous** → routed to a human, see below) |

A clean, fully-automated run produces **4 accepted / 5 rejected / 4 dropped**, plus **2**
rows routed to a human (`ledger.csv` gains 2 more once they're resolved — e.g. **6
accepted / 5 rejected / 4 dropped** if a reviewer identifies the unclear receipt's currency
and approves the smudged one). Without a reviewer available, both auto-resolve to "reject"
after a timeout (see below), landing at **4 accepted / 7 rejected / 4 dropped**.

### Why the validator is generated at runtime — and why lakeFS still gates

The per-receipt validation logic in Phase 3 is **not shipped in the repo** — the agent writes
it on the fly from a rule *specification* (`mount_receipts/validation.py: RULES_SPEC`) and
executes it inside the sandbox (`mount_receipts/codegen.py`). That is the point of running it
in E2B: **you are executing code you have never seen, generated at runtime, and the sandbox
contains whatever it does** — a self-repair loop re-prompts the model if its script crashes or
violates the I/O contract, with a reference implementation as a last-resort fallback so the
demo always completes. The exact generated script, its input, and its output are committed to
the branch under `validation/` so the run is fully auditable.

> **Why one rule stays deterministic.** The model judges each receipt *on its own*. The only
> cross-record rule — invoice-number uniqueness, which needs a whole-batch pass — is the one
> LLMs reliably get wrong, so the host owns it (`apply_cross_row_uniqueness`). The generated
> code still does the substantive per-receipt work; this just keeps the happy path reliable.

Because that validator is LLM-written and therefore *untrusted*, lakeFS does **not** take its
word for it. The **pre-merge Action** (`lakefs_actions/validate_ledger.yaml`) reads the typed
`validation/gate_input.json` the agent committed and **independently re-derives** accept/reject
for **every** drafted row — re-checking `total == sum(items)`, the \$500 cap, currency, ISO
date/year, **and** invoice-number uniqueness across the whole batch — then blocks the merge if
the validator accepted any row that violates policy (and cross-checks the accepted-row count).
It does not trust `validation/latest_result.json`'s status field. So a half-processed,
invalid, or self-certified ledger physically cannot be promoted to `main`, regardless of what
code the agent ran or who triggers the merge. **E2B is the compute trust boundary (run unknown
code safely); lakeFS is the data trust boundary (unknown data can't reach `main`).**

### See the gate fire

In the happy path the gate re-validates and the clean ledger merges. To watch it *block* a bad
ledger, set `DEMO_TAMPER=1`: after validation passes, one accepted row is corrupted to a
non-USD currency (the agent still self-reports "passed"), and the lakeFS gate independently
rejects the merge — nothing reaches `main`.

```bash
DEMO_TAMPER=1 uv run python -m mount_receipts.main   # gate blocks the merge
```

### Human-in-the-loop: pause on ambiguity

Not every case is a clean accept/reject. A low-contrast or smudged scan of a receipt may be
too illegible to trust — and not just in one predictable spot. Rather than guess or force a
default call, **the agent itself** — as part of the same generated validator, per rule 7 of
`RULES_SPEC` — uses its own judgement on whether a case is ambiguous, and flags it instead of
guessing an accept or reject.

Rule 7 is deliberately open-ended, not a fixed checklist of fields: "flag it whenever the
scan itself is too unclear to trust a value on it, judged broadly, the same way a person
skimming the image would." In the sample set that shows up two ways — a currency symbol too
generic or illegible to trust (a bare `$` says nothing about USD vs. CAD vs. SGD; a
low-contrast scan might not show a currency mark at all), and a total that's off from the
line items by an amount too small to be fraud but too large to ignore, a likely misread
digit — but those are illustrative, not the full list; a genuinely illegible vendor name or
date qualifies just as much. The host **never re-derives or overrides** the agent's call;
whatever the generated validator marks `ambiguous`, for whatever reason it gives, goes to a
human as-is. (A currency that *is* confidently read as non-USD is still a plain, immediate
reject — that's the agent's own judgment too, not a host-side check second-guessing it.)

When Phase 3 finds any `ambiguous` rows, the host (`orchestrator.py`) **pauses the E2B
sandbox** (`sbx.beta_pause()` — a VM-level pause; no compute is billed while paused) and asks
a human about each one — in Slack if `SLACK_BOT_TOKEN`/`SLACK_HUMAN_CHANNEL` are configured,
polling the thread for a reply, or an interactive prompt otherwise:

```
receipt_nocurrency.jpg: the agent's validator flagged this ambiguous — missing currency.
Approve, reject, or — if this is a currency issue — name the actual currency
(EUR, GBP, CHF, CAD, or USD).
```

A person answers whenever they get to it — the sandbox just sits paused until a reply comes
in (or `HUMAN_REVIEW_TIMEOUT_S` elapses, at which point it auto-decides
`HUMAN_REVIEW_DEFAULT`, keeping a non-interactive run from blocking forever). Once decided,
the host **resumes the exact same sandbox** (`Sandbox.connect(sandbox_id)`), writes the
decision into the mount, and re-runs Phase 3 — which now finalizes those rows and commits a
"Phase 3b — Human review" commit recording what was asked and decided
(`validation/human_review_log.json`).

There's no generic "convert" button — the validator has no network access and can't look up
an exchange rate itself (see `RULES_SPEC`'s I/O contract: "Do NOT use the network"), so
identifying the currency *is* the decision. Naming a recognized currency (`validation.FX_TO_USD`
— a small, fixed, illustrative rate table, not a live feed) converts the row to USD; naming
"USD" needs no conversion; anything else falls back to a reject.

> **"Approve" doesn't resolve anything.** Approving a still-unclear currency leaves it
> untouched — the (unchanged) pre-merge gate independently re-derives `currency == USD` from
> the data itself and will still **block the merge**, exactly like `DEMO_TAMPER` above. Only
> correctly identifying the currency or `"reject"` changes the merge outcome. No one — not
> even a human reviewer — bypasses the policy lakeFS enforces.

---

## Architecture

```
                 ┌─────────────────────┐
                 │  OpenAI gpt-4o      │  (called from inside the sandbox)
                 │  vision extraction  │
                 └─────────┬───────────┘
                           │ per-file JSON
                           ▼
┌──────────────────────────────────────────────────────────┐
│            E2B Sandbox  (mount + agent run as root)        │
│   everest mount lakefs://repo/<branch>/ /home/user/mnt     │
│                       --protocol fuse --write-mode         │
│                                                            │
│   inbox/*.{jpg,png,webp,bmp,tiff,pdf}   (plain file I/O)   │
│   archive/**                (large; never opened → lazy)   │
│   sidecars/<file>.json      (extraction cache)             │
│   validation/generated_validator.py  ← LLM-written, run    │
│                                         here as a subprocess│
│   ledger.csv / rejects.csv  (agent writes)                 │
│   validation/latest_result.json                            │
│                                                            │
│   host runs each phase, then:  everest commit -m "Phase N" │
└───────────────────────────┬────────────────────────────────┘
                            │ commits land on the branch
                            ▼
                 ┌──────────────────────┐
                 │      lakeFS          │
                 │  • agent branch      │
                 │  • pre-merge gate    │  ← re-derives every row's
                 │                      │    verdict from gate_input.json
                 │                      │    (doesn't trust the status)
                 └──────────┬───────────┘
                            │ merge only when the gate passes
                            ▼
                         main branch
```

The **host** (`orchestrator.py`) owns lakeFS branch creation, the per-phase `everest commit`,
and the final merge. The **agent** (`agent_runner.py`, run inside the sandbox) owns all the
work on the mounted files. Reads are **lazy** — a file's bytes are fetched from the object
store only when the agent opens it, straight from storage (the lakeFS server isn't in the
data path).

---

## Lazy fetch — bring a big dataset, pay only for what you open

Because reads are lazy, you can mount a branch holding a huge dataset and the agent only
fetches the handful of files it actually opens. `scripts/upload_archive.py` uploads a large
`archive/` of decoy files (tunable) that the agent never touches:

```bash
# default ~250 MB; scale freely (one-time, bandwidth-bound)
ARCHIVE_FILES=5000 ARCHIVE_SIZE_KB=1024 uv run python scripts/upload_archive.py   # ~5 GB
```

Measured with a 5 GB / 5,000-file `archive/` present:

| Metric | Value |
|---|---|
| Mounted (branch, apparent) | **4.9 GB / 5,013 files** |
| everest local cache after the run | **~16 MB** (branch metadata for 5,000 files + the agent's inbox reads) |
| `archive/` content (4.9 GB) | **never fetched** |

Mounting is near-instant regardless of branch size; only the agent's inbox reads transfer. The
local cache tracks **file count** (metadata), not archive size — bump `archive/` to tens of GB and
the fetched content stays flat.

---

## Prerequisites

- A **lakeFS instance with Mount (`everest`) enabled** — sign up for [lakeFS Cloud](https://lakefs.cloud/) (Mount is available on lakeFS Cloud / Enterprise).
- An **E2B** account — sign up at [e2b.dev](https://e2b.dev) and get your key from the [dashboard](https://e2b.dev/dashboard) (`E2B_API_KEY`).
- An **OpenAI** key (`gpt-4o` for vision).
- Optional: a **Slack** bot token (scopes `chat:write` + `channels:history`/`groups:history`)
  to answer "ambiguous" rows in Slack (`SLACK_BOT_TOKEN`/`SLACK_HUMAN_CHANNEL`). Without one,
  the demo still runs end-to-end — it falls back to an interactive CLI prompt, or
  auto-decides `HUMAN_REVIEW_DEFAULT` when run non-interactively.
- The **everest Linux x86_64 binary** at `build/bin/everest`. `everest` is the lakeFS
  **Enterprise** Mount binary — it is **not redistributable**, so it is gitignored and not
  shipped with this sample. Download it yourself from the lakeFS changelog (or
  [contact lakeFS](https://lakefs.io/contact-sales/)) and extract:
  ```bash
  curl -fsSL -o everest.tgz https://artifacts.lakefs.io/everest/0.9.2/everest_0.9.2_Linux_x86_64.tar.gz
  mkdir -p build/bin && tar -xzf everest.tgz -C build/bin
  ```

Copy `.env.example` to `.env` and fill it in. `.env` is gitignored.

---

## Quickstart

```bash
# from the repo root: install the workspace
uv sync
cd usecases/03-mount-receipts
cp .env.example .env        # fill in lakeFS + E2B + OpenAI

# 1) create the repo (if needed) + install the pre-merge gate on main
uv run python scripts/setup_lakefs_repo.py
# 2) generate the messy receipt inbox and upload it to main/inbox/
uv run python scripts/upload_sample_data.py
# 3) (optional) upload a large archive/ to show lazy fetch
uv run python scripts/upload_archive.py
# 4) run the agent (branch → sandbox → mount → 3 phases → commit → gate → merge)
uv run python -m mount_receipts.main
```

Or steps 1–2 + run at once: `uv run python scripts/run_demo.py`.

Run the unit tests (no live services): `uv run pytest`.

---

## Inspecting results

```bash
lakectl fs cat   lakefs://<repo>/main/ledger.csv      # accepted receipts
lakectl fs cat   lakefs://<repo>/main/rejects.csv     # dropped/rejected, with reasons
lakectl log      lakefs://<repo>/<agent-branch>       # progressive phase commits (+ a
                                                       # "Phase 3b — Human review" commit
                                                       # whenever a row was ambiguous)
```

**E2B ↔ lakeFS link.** `everest commit` can't attach lakeFS commit metadata, so after the
phase commits the host writes a final **"Agent run manifest"** commit carrying a clickable
**E2B Sandbox** link (`::lakefs::E2B Sandbox::url[url:ui]` metadata, shown when `E2B_TEAM` is
set) plus a `run_manifest.json`. From any merged ledger you can jump straight to the exact
sandbox that produced it — the audit trail spans both systems.

**See the gate block a bad ledger:** create a branch, write a `validation/latest_result.json`
with `"status":"failed"`, commit, and `lakectl merge` it into `main` — the pre-merge Action
rejects it with `412 Precondition Failed`.

### Browse the live mount in the E2B dashboard

Run with `--keep-sandbox` to leave the sandbox (and its mount) alive, then open the sandbox's
**Filesystem** tab and navigate to `/home/user/mnt` to see the lakeFS branch as live files:

```bash
uv run python -m mount_receipts.main --keep-sandbox   # report prints the E2B sandbox link
```

**Why the agent runs as root:** E2B's `envd` (which serves the dashboard Filesystem tab) runs
as root, but a FUSE mount is only visible to the user that mounted it, and `everest` has no
`allow_other` option. So the mount + agent run as root (`sudo -E`) so the mount is root-owned
and the tab can read it. (A user-mounted branch shows up empty in the tab — a good joint
feature request for E2B/lakeFS; see `PRESENTATION.md`.)

---

## Faster startup: prebaked E2B template (optional)

By default the sandbox does a one-time runtime install of `everest` + `fuse` + python deps.
A prebaked template removes that. Measured `create + provision` (ready-to-mount), n=3:

| Path | Provision | Ready |
|---|---|---|
| Runtime install | ~20.9s | ~21.1s |
| Template (`mount-receipts`) | ~0.9s | ~2.7s |

Build it once and point `.env` at it:

```bash
cd template
cp ../build/bin/everest ./everest      # binary into the build context (gitignored)
e2b template build --name mount-receipts   # requires `e2b auth login`
# then set E2B_TEMPLATE=mount-receipts in .env
```

The template bakes `fuse3` + the everest binary + python deps (installed system-wide so the
root-run agent can import them); the agent package is uploaded fresh each run, so the template
never ships stale code. Reproduce the numbers with `scripts/benchmark_provision.py`.

---

## Layout

```
src/mount_receipts/
  config.py         env-driven config
  sample_data.py    synthetic messy inbox generator (multi-format) + expected manifest
  extraction.py     gpt-4o vision extraction + PDF rasterization/page-stacking (PyMuPDF)
  validation.py     three-phase rules + ledger completeness (pure; unit-tested)
  agent_runner.py   the in-sandbox agent: triage / extract / validate over the mount
  e2b_session.py    sandbox provisioning + everest mount/commit/umount/pause/resume (as root)
  human_review.py   host-only: ask a human about "ambiguous" rows (Slack, or CLI fallback)
  orchestrator.py   host: branch → reset outputs → sandbox → phases → (pause/ask/resume) → manifest → gate → merge
  main.py           CLI entry point (--no-merge, --branch, --keep-sandbox)
scripts/            setup_lakefs_repo · upload_sample_data · upload_archive · run_demo · benchmark_provision
lakefs_actions/     validate_ledger.yaml  (pre-merge gate)
template/           e2b.Dockerfile + e2b.toml  (optional prebaked sandbox)
tests/              validation unit tests
PRESENTATION.md     E2B-facing demo guide (story, architecture, measured numbers, demo script)
```
