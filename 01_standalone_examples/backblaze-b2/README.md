# lakeFS on Backblaze B2

Start by ⭐️ starring [lakeFS Community](https://go.lakefs.io/oreilly-course) project.

This sample runs lakeFS Enterprise with [Backblaze B2](https://www.backblaze.com/cloud-storage) as its
underlying storage, and uses [lakeFS Datasets](https://docs.lakefs.io/datasets/) to publish a curated,
versioned slice of that data.

B2 exposes an S3-compatible API, so lakeFS talks to it as an `s3` blockstore. Everything the demo
produces — object data, lakeFS commit metadata, and the dataset's own backing storage — lives in your
B2 bucket. Nothing is stored locally.

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
  [Contact Sales](https://lakefs.io/contact-sales/) for a license and a Docker Hub token.

## Setup

1. Clone this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples
   cd lakeFS-samples/01_standalone_examples/backblaze-b2
   ```

3. Create a Backblaze B2 Application Key.

   In the B2 console, go to **Application Keys** and create a new key.

   * **Do not use the Master Application Key** — it is not supported by the S3-compatible API.
   * If you scope the key to a single bucket, it also needs `listAllBucketNames`, or some SDKs
     will fail to list.
   * Note your **region** — your endpoint is shown on the **Buckets** page as
     `s3.<region>.backblazeb2.com`. The sample wants the `<region>` part, for example `us-east-005`,
     not the display name ("US East") and not the full hostname.

4. Copy `.env.example` to `.env` and fill it in:

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

5. Start the stack:

   ```bash
   docker compose up
   ```

   If port 8085 or 8895 is already in use, change it in `docker-compose.yml`.

6. Open JupyterLab at [http://127.0.0.1:8895/](http://127.0.0.1:8895/) and run
   **"lakeFS on Backblaze B2"**.

### URLs and login details

* Jupyter [http://127.0.0.1:8895/](http://127.0.0.1:8895/)
* lakeFS [http://127.0.0.1:8085/](http://127.0.0.1:8085/)
  (`AKIAIOSFOLKFSSAMPLES` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`)

## Configuring lakeFS for Backblaze B2

B2 needs three settings that are not the S3 defaults. Without them lakeFS will not connect at all:

```yaml
blockstore:
  type: "s3"
  s3:
    endpoint: "https://s3.<region>.backblazeb2.com"
    region: "<region>"
    force_path_style: true          # B2 does not serve virtual-host style buckets
    discover_bucket_region: false   # the bucket-region probe is an AWS-ism
```

The `docker-compose.yml` in this folder sets the equivalent environment variables from your `.env`.

### A note on Iceberg and Metadata Search

lakeFS features that depend on the Iceberg commit protocol — the **Iceberg catalog** and
**Metadata Search** — are **not usable on B2 today**. They rely on S3 conditional writes
(`If-None-Match` / `If-Match`), which B2 has in limited preview only, with broader rollout expected
from Q1 2027.

Worth knowing: B2 does not reject a conditional write with a clean `501`. It closes the connection
mid-response, so clients report a transport error rather than an unsupported feature, which makes it
easy to misdiagnose as a network problem.

Datasets, used in this demo, does not depend on conditional writes.

## Demo Instructions

Once the setup is complete, open **"lakeFS on Backblaze B2"** from the JupyterLab UI and follow the
instructions in the notebook.
