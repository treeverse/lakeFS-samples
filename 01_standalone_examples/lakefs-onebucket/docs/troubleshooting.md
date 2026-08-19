# Troubleshooting

## lakeFS container exits immediately

**Check logs:**
```bash
docker compose logs lakefs
```

Common causes:
- `LAKEFS_AUTH_ENCRYPT_SECRET_KEY` is empty — generate one: `openssl rand -hex 32`
- `ONEBUCKET_ENDPOINT` is missing or malformed (must include `http://` or `https://`)
- Postgres not yet ready — lakeFS depends on the postgres healthcheck, but Docker Compose
  health propagation can occasionally race. Try `make down && make up`.

---

## `make check-onebucket` fails with TLS error

Set in `.env`:
```
ONEBUCKET_SKIP_VERIFY=true
```
This sets `LAKEFS_BLOCKSTORE_S3_SKIP_VERIFY_HTTPS=true` in the lakeFS container and
`verify=False` in the Python boto3 client.

> Only use this if OneBucket uses a self-signed or private CA certificate.

---

## `make check-onebucket` fails with "Connection refused" or "Cannot resolve hostname"

- Verify `ONEBUCKET_ENDPOINT` is reachable from your machine (not just inside Docker).
- If the endpoint is only accessible from inside Docker, run the check inside a container:
  ```bash
  docker compose run --rm lakefs wget -qO- ${ONEBUCKET_ENDPOINT} || true
  ```
- Ensure the URL scheme is present: `https://...` not `example.onebucket.endpoint`.

---

## `make check-onebucket` fails with "Bucket does not exist" (404)

- Check `ONEBUCKET_BUCKET` — make sure the bucket name is correct and does not include
  a path or the endpoint hostname.
- Verify the bucket exists in OneBucket.

---

## `make check-onebucket` fails with "Access denied" (403)

- Credentials lack permission on the bucket. Verify in OneBucket that the key has at
  minimum: `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket`,
  `s3:HeadBucket`.

---

## `make init` times out waiting for lakeFS

- Check that `make up` completed: `docker compose ps`
- Watch startup: `make logs`
- The lakeFS container may be crashing — check: `docker compose logs lakefs`

---

## `make init` fails with authentication error after lakeFS starts

The lakeFS container bootstraps the admin user by running `lakefs setup` before
starting the server (see the `command:` block in `docker-compose.yml`). If
authentication still fails:

- Verify `LAKEFS_ACCESS_KEY_ID` and `LAKEFS_SECRET_ACCESS_KEY` are set in `.env`.
- Check whether setup reported an error: `docker compose logs lakefs | head -40`
- Re-run setup by hand:
  ```bash
  docker compose exec lakefs /app/lakefs setup \
    --user-name admin \
    --access-key-id YOUR_KEY \
    --secret-access-key YOUR_SECRET
  ```
- If lakeFS was previously initialised with different credentials (stale Postgres volume),
  do a full reset: `make clean && make up && make init`

**Why not `LAKEFS_INSTALLATION_*`?** Those variables only trigger auto-setup when
`LAKEFS_DATABASE_TYPE=local`. This example runs on PostgreSQL, where they are ignored
entirely — so the container calls `lakefs setup` explicitly instead.

---

## `make init` fails with "storage namespace already in use"

```
failed to create repository: found lakeFS objects in the storage
namespace(s3://your-bucket/your-prefix/) key(_lakefs/dummy): storage namespace already in use
```

lakeFS refuses to create a repository over a prefix that already holds lakeFS data.
This usually means the repository was created before and its metadata is still in
OneBucket — most often after `make clean`, which removes the PostgreSQL volume but
does **not** touch the bucket.

Pick one:

- **Point at a fresh prefix** — change `LAKEFS_STORAGE_NAMESPACE` in `.env`
  (for example `s3://your-bucket/demo-2/`) and re-run `make init`.
- **Reuse the existing repository** — if the repo still exists in lakeFS, set
  `CREATE_LAKEFS_REPO=false` in `.env` and `make init` will validate it instead
  of creating it.
- **Clear the prefix** — delete the objects under that namespace in OneBucket,
  then re-run `make init`. This permanently destroys the repository's data.

---

## `make demo` fails with `InvalidBucketName` or 400 error

- The `LAKEFS_REPO` repository may not exist yet. Run `make init` first.
- Ensure `LAKEFS_REPO` contains only lowercase letters, numbers, and hyphens.

---

## `make demo` fails with checksum-related errors on PutObject

boto3 1.35+ sends `x-amz-checksum-*` headers that some lakeFS versions reject.
The scripts already set:
```python
Config(
    request_checksum_calculation="when_required",
    response_checksum_validation="when_required",
)
```
If you still see checksum errors, downgrade boto3: `pip install boto3==1.34.162`.

---

## lakeFS cannot write to OneBucket (blockstore errors)

Check `docker compose logs lakefs` for errors like `failed to put object`.

Common causes:
- `ONEBUCKET_FORCE_PATH_STYLE=false` — set to `true` for most S3-compatible stores.
- Chunked transfer encoding rejected — already disabled via
  `LAKEFS_BLOCKSTORE_S3_STREAMINGCHUNKEDENCODING=false` in `docker-compose.yml`.
- TLS verification failing inside the container — set `ONEBUCKET_SKIP_VERIFY=true`.
- `ONEBUCKET_REGION` mismatch — try `us-east-1` or whatever the endpoint expects.

---

## Objects visible in lakeFS but missing in OneBucket

lakeFS writes blockstore objects under the path set in `LAKEFS_STORAGE_NAMESPACE`.
Check inside OneBucket that objects exist under that prefix.

---

## `make clean` warning: volumes still in use

Stop all containers first: `docker compose down`, then `make clean`.

---

## Port 8000 already in use

Change the host port in `docker-compose.yml`:
```yaml
ports:
  - "8001:8080"
```
Then update `LAKEFS_ENDPOINT=http://localhost:8001` in `.env`.
