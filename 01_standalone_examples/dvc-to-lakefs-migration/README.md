# DVC to lakeFS Migration Demo

An end-to-end, self-contained sample that migrates a Git-backed [DVC](https://dvc.org) project into
[lakeFS](https://lakefs.io) using the
[`dvc-to-lakefs`](https://github.com/treeverse/dvc-to-lakefs) package, and then validates the result.

Everything runs locally with Docker Compose: [MinIO](https://min.io) as the object store, lakeFS
backed by MinIO, and a Jupyter notebook that drives the migration.

## What this demo proves

- A Git-backed DVC project with both a `dvc add` output and a `dvc.yaml` pipeline output migrates into lakeFS.
- Two Git branches (`main` and `experiment`) become lakeFS branches of the same name.
- The migration is **zero-copy**: imported objects are referenced from the DVC remote, not copied.
- File contents read back through lakeFS match the contents tracked on the corresponding DVC branch.
- Each lakeFS commit records the source Git SHA in its `git_sha` commit metadata.

## How the migration works (read this first)

`dvc-to-lakefs` performs a **zero-copy import by reference**. DVC has already pushed your object
content to its remote with `dvc push`; the migration does not re-upload anything. Instead, it imports
the HEAD of each Git branch into lakeFS by **referencing the existing objects** in the DVC remote.

The practical consequence: the lakeFS repository's storage namespace and the DVC remote must be on
the **same object store**. In this demo, both use the same local MinIO instance.

## Prerequisites

- Docker
- Docker Compose

The published sample installs `dvc-to-lakefs` from PyPI automatically — no local checkout required.

## Run it

```bash
docker compose up --build
```

Then open the notebook and run all cells:

- Jupyter: http://127.0.0.1:8888/lab/tree/work — open **`DVC to lakeFS Migration.ipynb`**

## Clean up

```bash
docker compose down -v
```

This also removes the MinIO and lakeFS data. The generated DVC project under `demo-workdir/` can be
deleted separately; it is git-ignored.

## Local URLs and credentials

These are local demo credentials only.

| Service | URL | Credentials |
|---|---|---|
| lakeFS Web UI | http://127.0.0.1:8000 | `AKIAIOSFOLKFSSAMPLES` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` |
| MinIO Console | http://127.0.0.1:9001 | `minioadmin` / `minioadmin` |

## The command to notice

The entire migration is a single command, run inside the notebook:

```bash
lakectl-import-from-dvc <dvc-repo> lakefs://dvc-migration-demo --branch main --branch experiment
```

Add `--dry-run` to preview the import plan without writing anything to lakeFS. The package is also
invokable as `lakectl import-from-dvc ...` (lakectl plugin) or `python -m dvc_to_lakefs ...`.

## Verifying the migration — how the pieces fit

Because the import is zero-copy, the same object bytes are referenced from all three layers. You can
follow a single file across them:

```
DVC pointer (data/dataset.csv.dvc)   md5: bbd304055be67c211495c95f4e697eb2
        │  Git tracks this tiny pointer, not the data
        ▼
MinIO blob   dvc-remote/cache/files/md5/bb/d304055be67c211495c95f4e697eb2
        ▲  the real bytes, uploaded once by `dvc push`
        │
lakeFS object  data/dataset.csv @ branch main  →  physical_address points to the MinIO blob above
```

**See it in lakeFS** — open http://127.0.0.1:8000, browse `dvc-migration-demo`, switch between the
`main` and `experiment` branches, and open a commit to find the original Git message and the
`git_sha` commit metadata.

**See it in MinIO** — open http://127.0.0.1:9001 and look in the `dvc-remote` bucket under
`cache/files/md5/...`. Those hashed objects are the data; the `lakefs-storage` bucket holds only
lakeFS metadata, never copies of your data.

**See it in DVC** — open a terminal (in Jupyter: *File ▸ New ▸ Terminal*, or
`docker compose exec jupyter-notebook bash`) and inspect the generated repo:

```bash
cd ~/demo-workdir/source-dvc-repo
cat data/dataset.csv.dvc        # the pointer Git tracks (md5 + size, not the data)
git checkout main && dvc checkout
cat data/dataset.csv            # the data DVC restores from the remote (3 rows)
dvc remote list                 # where the bytes live: s3://dvc-remote/cache
git checkout experiment && dvc checkout && cat data/dataset.csv   # 5 rows on this branch
```

The MD5 in a `.dvc` pointer is exactly the MinIO object path (sharded by its first two characters)
and exactly the `physical_address` lakeFS references — one set of bytes, three views.

## Migration preconditions

For a migration to succeed against any DVC repository:

- The DVC repository must be **Git-backed** (`dvc init --no-scm` is not supported).
- The DVC data must already be pushed to the DVC remote (`dvc push`).
- The lakeFS repository must be backed by the **same object-store family** as the DVC remote
  (e.g. both on S3), because the import references the DVC remote's objects directly.
- For this demo, DVC and lakeFS both use the same local MinIO instance.

## What is intentionally not demonstrated

- Full Git history replay — only the HEAD of each branch is imported.
- Deletion propagation — removing a `.dvc` output and re-importing does not delete it from lakeFS.
- Cloud-specific authentication (S3/GCS/Azure credentials, roles, etc.).
- Production lakeFS deployment hardening.

## How to adapt this to your own DVC repo

1. Point lakeFS at the object store your DVC remote already uses, and create a lakeFS repository whose
   storage namespace lives on that same object store.
2. Make sure your DVC data is pushed: `dvc push`.
3. Configure lakeFS credentials in `~/.lakectl.yaml` (or set `LAKECTL_*` environment variables).
4. Run the migration against your repository, listing the branches you want:

   ```bash
   lakectl-import-from-dvc /path/to/your/dvc-repo lakefs://<your-repo> \
       --branch main --branch <other-branch>
   ```

5. Use `--dry-run` first to review the plan. See the
   [`dvc-to-lakefs` README](https://github.com/treeverse/dvc-to-lakefs) for all options and the list
   of supported remotes and unsupported output types.

## Developing against a local `dvc-to-lakefs` checkout

By default the image installs `dvc-to-lakefs` from PyPI. To build against a local checkout instead:

```bash
cp docker-compose.override.yml.example docker-compose.override.yml
# edit the volume path to point at your checkout, then:
docker compose up --build
```

`docker-compose.override.yml` is git-ignored.

## Troubleshooting

- **Docker is not running** — start Docker Desktop (or your Docker daemon) and retry.
- **Ports already in use** (`8000`, `8888`, `9000`, `9001`) — stop the conflicting process or remap
  the host ports in `docker-compose.yml`.
- **`dvc push` / migration cannot find data** — the notebook pushes data before migrating; if you
  adapt it, make sure `dvc push` ran and the remote is reachable.
- **lakeFS repository already exists** — repository creation in the notebook is idempotent and reuses
  an existing repository; re-running the migration never deletes existing files.
- **`dvc-to-lakefs` not found when using a local checkout** — confirm the volume path in
  `docker-compose.override.yml` points at your checkout and rebuild with `--build`.
