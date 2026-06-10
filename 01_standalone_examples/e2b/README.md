# Agentic Data Patterns: E2B + lakeFS

> A collection of self-contained demos showing the same core idea from different
> angles: **an AI agent executes in an isolated E2B sandbox and versions its work
> in lakeFS** — every iteration committed, nothing reaching `main` until it passes
> a server-enforced gate.

E2B gives the agent **compute isolation** (generated code can't touch your network
or infra). lakeFS gives it **data isolation and version control** (each run gets
its own branch; intermediate and failed states never touch production; a pre-merge
Action enforces promotion rules regardless of who triggers the merge).

---

## Prerequisites

All use cases need an E2B account and a lakeFS instance:

- **E2B** — sign up at [e2b.dev](https://e2b.dev) and grab your API key from the
  [dashboard](https://e2b.dev/dashboard).
- **lakeFS** — the easiest path is [lakeFS Cloud](https://lakefs.cloud/) (free sign-up);
  a self-hosted instance reachable from the internet works too. **Use case 3 additionally
  requires lakeFS Mount (`everest`)**, which is available on lakeFS Cloud / Enterprise.
- **OpenAI** — use cases 2 and 3 call an OpenAI model (`OPENAI_API_KEY`).

Each use case has its own `.env.example` listing exactly what it needs.

---

## Use cases

| # | Directory | Pattern | Data path |
|---|-----------|---------|-----------|
| 1 | [`usecases/01-sdk-orchestrator`](usecases/01-sdk-orchestrator) | Orchestrated agent job: isolated compute + isolated data branch | lakeFS **Python SDK** |
| 2 | [`usecases/02-three-phase-repair`](usecases/02-three-phase-repair) | Iterative LLM data repair through three progressive validation phases | lakeFS **Python SDK** |
| 3 | [`usecases/03-mount-receipts`](usecases/03-mount-receipts) | Multimodal receipt → ledger curation; agent works on a mounted filesystem | lakeFS **Mount (`everest`)** |

**SDK vs Mount.** Use cases 1–2 move data via the lakeFS SDK (read object →
process → write object → commit). Use case 3 uses **lakeFS Mount**, exposing the
branch as a real filesystem — so the agent does plain file I/O (`open`, `glob`,
`write`) with no SDK or S3 knowledge, which is how most agents naturally operate.
Versioning happens via `everest commit`. Demo 3 needs a lakeFS instance with
Mount (`everest`) enabled.

---

## Layout

```
.
├── shared/lakefs_e2b_common/   # shared plumbing: lakeFS SDK wrappers + env loading
├── usecases/
│   ├── 01-sdk-orchestrator/    # use case 1  (package: lakefs_e2b_poc)
│   ├── 02-three-phase-repair/  # use case 2  (package: agent_demo)
│   └── 03-mount-receipts/      # use case 3  (package: mount_receipts)
├── pyproject.toml              # uv workspace root (members: shared + usecases/*)
└── README.md
```

This is a **uv workspace**: one `.venv` shared across all members, each installed
editable. The shared package (`lakefs_e2b_common`) holds the lakeFS SDK wrappers
and env-loading helpers; use-case-specific logic (validation rules, agent loops,
sandbox runners, reporting) lives inside each use case.

> Use case 2 imports the shared plumbing. Use case 1 was relocated as-built
> (self-contained) and can be migrated onto `lakefs_e2b_common` once it's been
> re-run against a live lakeFS instance.

---

## Quickstart

```bash
# Install everything (shared + all use cases) into one .venv
uv sync

# Each use case has its own README, .env.example, and run instructions:
cat usecases/02-three-phase-repair/README.md
```

Each use case reads its own `.env` (copy from the local `.env.example`). Secrets
are never committed — `.env` files are gitignored.

---

## History

Use cases 1 and 2 previously lived on separate branches (`main` and `demo-v2`).
Those branches are preserved for reference; this monorepo layout supersedes them.
