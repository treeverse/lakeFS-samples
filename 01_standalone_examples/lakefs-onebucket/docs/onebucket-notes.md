# Configuration notes: lakeFS on an S3-compatible blockstore

This demo points lakeFS at OneBucket over the S3 API. The settings below are what make
that work, and why. They apply to most S3-compatible object stores, not just OneBucket.

---

## Path-style addressing

Set `ONEBUCKET_FORCE_PATH_STYLE=true` (the default in `.env.example`).

Virtual-hosted-style addressing (`bucket.endpoint/key`) requires DNS wildcard entries
that many S3-compatible deployments do not have. Path-style (`endpoint/bucket/key`) is
broadly supported.

In `docker-compose.yml` this maps to:
```
LAKEFS_BLOCKSTORE_S3_FORCE_PATH_STYLE: true
```

In the boto3 clients (both `check_onebucket.py` and `run_demo.py`):
```python
Config(s3={"addressing_style": "path"})
```

---

## TLS / self-signed certificates

If your endpoint uses a self-signed or private CA certificate:
- Set `ONEBUCKET_SKIP_VERIFY=true` in `.env`.
- This propagates to:
  - `LAKEFS_BLOCKSTORE_S3_SKIP_VERIFY_HTTPS=true` in the lakeFS container.
  - `verify=False` in the boto3 client in `check_onebucket.py`.

> This is a convenience for local demos. For production, configure a proper certificate
> chain rather than disabling verification.

---

## Region

Many S3-compatible endpoints accept any region string. The default `us-east-1` usually
works. If your endpoint requires a specific region, set `ONEBUCKET_REGION=<region>` in
`.env` — it flows to both `LAKEFS_BLOCKSTORE_S3_REGION` and the boto3 session region.

This demo also sets:
```
LAKEFS_BLOCKSTORE_S3_DISCOVER_BUCKET_REGION: "false"
```

Automatic region discovery calls `GetBucketLocation`, which S3-compatible endpoints do
not always implement. Setting the region explicitly and disabling discovery avoids
depending on it.

---

## Chunked/streaming transfer encoding

Some S3-compatible APIs do not accept AWS Signature V4 chunked (streaming) encoding.
`docker-compose.yml` disables it for lakeFS:
```
LAKEFS_BLOCKSTORE_S3_STREAMINGCHUNKEDENCODING: "false"
```

The boto3 clients in `check_onebucket.py` and `run_demo.py` also disable automatic
checksum negotiation for the same reason:
```python
request_checksum_calculation="when_required",
response_checksum_validation="when_required",
```

---

## Storage namespace layout

lakeFS writes all of its blockstore objects (data blocks + metadata manifests) under
`LAKEFS_STORAGE_NAMESPACE`.

Recommended layout for multiple demos or repositories:
```
s3://my-bucket/lakefs/
  onebucket-demo/
    data/
    _lakefs/
  other-repo/
    ...
```

The default derived namespace is `s3://<ONEBUCKET_BUCKET>/lakefs/<LAKEFS_REPO>/`.
Using a prefix keeps lakeFS objects from mixing with other bucket content and makes
cleanup straightforward: delete the prefix.

---

## What lakeFS writes to the bucket

lakeFS writes two categories of objects to the blockstore:

1. **Data blocks** — content-addressable chunks of the files committed through lakeFS.
   Written at commit time. Path example: `<namespace>/data/<id>`.

2. **Metadata** — range and meta-range files encoding the committed tree.
   Path example: `<namespace>/_lakefs/`.

`run_demo.py` does **not** write directly to the object store. All data flows through
the lakeFS S3 Gateway, which manages the blockstore writes internally.

---

## Connectivity check prefix

`check_onebucket.py` writes and deletes a single test object under:
```
_lakefs_demo_connectivity_check/
```

This is the only script that talks directly to the object store. It is safe to re-run;
it cleans up after itself.

---

## If something behaves unexpectedly

lakeFS needs a fairly small slice of the S3 API. If a call fails, check
`docker compose logs lakefs` and confirm the credentials have `s3:GetObject`,
`s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket`, and multipart upload permissions on
the bucket. Bucket versioning is not required — lakeFS manages its own versioning layer.

See [troubleshooting.md](troubleshooting.md) for specific errors and fixes.
