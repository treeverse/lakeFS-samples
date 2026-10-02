# lakeFS on Backblaze B2

Start by ⭐️ starring [lakeFS Community](https://go.lakefs.io/oreilly-course) project.

This sample runs lakeFS Enterprise with [Backblaze B2](https://www.backblaze.com/cloud-storage) as its
underlying storage, and uses [lakeFS Datasets](https://docs.lakefs.io/datasets/) to publish a curated,
versioned slice of that data.

B2 exposes an S3-compatible API, so lakeFS talks to it as an `s3` blockstore. The objects, the files
lakeFS writes to describe each commit, and the dataset's own backing storage all live in your B2
bucket. lakeFS keeps its own records of repositories, branches and commits in the local Postgres
container.

* In this demo you will:
1. Confirm lakeFS is really using B2 as its blockstore
2. Version data on B2 — branch, commit, merge
3. Read the objects straight out of B2 with `boto3`, bypassing lakeFS, to see where they actually live
4. Publish a **Dataset**: an immutable, versioned slice pinned to a commit, that you can share instead of
   handing over the whole repository
5. Publish a second version of that dataset as the data grows

## Prerequisites

* Docker installed on your local machine
* A Backblaze B2 account and a bucket
* A lakeFS Enterprise license with the `datasets` capability.
  [Contact Sales](https://lakefs.io/contact-sales/) for a license.

## Setup

1. Clone this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples
   cd lakeFS-samples/01_standalone_examples/backblaze-b2
   ```

2. Create a Backblaze B2 Application Key.

   In the B2 console, go to **Application Keys** and create a new key.

   * **Do not use the Master Application Key** — it is not supported by the S3-compatible API.
   * A key scoped to a single bucket works for this sample. If another tool you use with it fails
     to list buckets, give the key `listAllBucketNames` as well.
   * Note your **region** — your endpoint is shown on the **Buckets** page as
     `s3.<region>.backblazeb2.com`. The sample wants the `<region>` part, for example `us-east-005`,
     not the display name ("US East") and not the full hostname.

3. Copy `.env.example` to `.env` and fill it in:

   ```bash
   cp .env.example .env
   ```

   ```
   B2_KEY_ID=<keyID>
   B2_APP_KEY=<applicationKey>
   B2_REGION=us-east-005
   B2_BUCKET=<your bucket name>

   LAKEFS_LICENSE_FILE=/absolute/path/to/your/license.token
   LAKEFS_INSTALLATION_ID=<installation id from the license>
   ```

   `.env` is gitignored. **Keep your license file outside this repository** — the compose file mounts it
   read-only from wherever `LAKEFS_LICENSE_FILE` points, so it never needs to live in the source tree.

4. Start the stack:

   ```bash
   docker compose up
   ```

   If port 8085 or 8895 is already in use, change it in `docker-compose.yml`.

5. Open JupyterLab at [http://127.0.0.1:8895/](http://127.0.0.1:8895/) and run
   **"lakeFS on Backblaze B2"**.

### URLs and login details

* Jupyter [http://127.0.0.1:8895/](http://127.0.0.1:8895/)
* lakeFS [http://127.0.0.1:8085/](http://127.0.0.1:8085/)
  (`AKIAIOSFOLKFSSAMPLES` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`)

## Configuring lakeFS for Backblaze B2

B2 needs its regional endpoint. The other two settings below are not the S3 defaults. lakeFS will
connect without them, but they are the recommended setup for B2.

```yaml
blockstore:
  type: "s3"
  s3:
    endpoint: "https://s3.<region>.backblazeb2.com"
    region: "<region>"
    force_path_style: true          # B2 accepts both styles, path style is the usual choice
    discover_bucket_region: false   # B2 may refuse the region lookup, which lakeFS logs as an error
```

The `docker-compose.yml` in this folder sets the equivalent environment variables from your `.env`.

### A note on Iceberg and Metadata Search

lakeFS features that depend on the Iceberg commit protocol — the **Iceberg catalog** and
**Metadata Search** — are **not usable on B2 today**. They rely on S3 conditional writes
(`If-None-Match` / `If-Match`), which B2 has in limited preview only, with broader rollout expected
from Q1 2027.

One thing worth knowing is that B2 answers a conditional write with `501 NotImplemented`, but some
clients report it as a dropped connection instead. boto3 does this because it sends
`Expect: 100-continue`, and with that header removed it gets the `501` too. So a failed conditional
write can look like a network problem when it is really an unsupported feature.

Datasets, used in this demo, does not depend on conditional writes.

## Demo Instructions

Once the setup is complete, open **"lakeFS on Backblaze B2"** from the JupyterLab UI and follow the
instructions in the notebook.
