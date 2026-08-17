#!/usr/bin/env python3
"""
Validate OneBucket connectivity before starting lakeFS.

Performs a safe write/read/delete under the prefix:
  _lakefs_demo_connectivity_check/

Nothing outside that prefix is touched.
"""

import os
import sys
import urllib.parse
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load .env from the project root (one level up from scripts/)
load_dotenv(Path(__file__).parent.parent / ".env")

TEST_PREFIX = "_lakefs_demo_connectivity_check/"
TEST_KEY = f"{TEST_PREFIX}test.txt"
TEST_BODY = b"lakeFS OneBucket connectivity check"


def redact(value: str) -> str:
    if not value:
        return "(empty)"
    if len(value) <= 8:
        return "***"
    return value[:4] + "***" + value[-2:]


def validate_endpoint(raw: str) -> str:
    endpoint = raw.strip()
    if not endpoint:
        raise ValueError("ONEBUCKET_ENDPOINT is not set")
    if not endpoint.startswith(("http://", "https://")):
        raise ValueError(
            f"ONEBUCKET_ENDPOINT must start with http:// or https://\n"
            f"  Got: {endpoint!r}"
        )
    parsed = urllib.parse.urlparse(endpoint)
    if parsed.path and parsed.path not in ("/", ""):
        # Bucket name accidentally included in endpoint URL is a common mistake
        print(
            f"  WARNING: ONEBUCKET_ENDPOINT contains a path '{parsed.path}'.\n"
            "           The bucket name belongs in ONEBUCKET_BUCKET, not the endpoint URL."
        )
    return endpoint


def make_s3_client(
    endpoint: str,
    access_key: str,
    secret_key: str,
    region: str,
    force_path_style: bool,
    skip_verify: bool,
) -> boto3.client:
    if skip_verify:
        # Suppress InsecureRequestWarning when TLS verification is disabled
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    session = boto3.Session(
        aws_access_key_id=access_key.strip(),
        aws_secret_access_key=secret_key.strip(),
        region_name=region.strip() or "us-east-1",
    )
    return session.client(
        "s3",
        endpoint_url=endpoint,
        verify=not skip_verify,
        config=Config(
            s3={"addressing_style": "path" if force_path_style else "auto"},
            # Avoid checksum negotiation issues with S3-compatible endpoints
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def diagnose_connection_error(exc: Exception, endpoint: str, skip_verify: bool) -> None:
    msg = str(exc).lower()
    if "ssl" in msg or "certificate" in msg:
        if not skip_verify:
            print(f"  ERROR: TLS/SSL error connecting to {endpoint}")
            print("         Set ONEBUCKET_SKIP_VERIFY=true in .env if OneBucket uses a")
            print("         self-signed or private CA certificate.")
        else:
            print(f"  ERROR: SSL error even with verify disabled: {exc}")
    elif "connection refused" in msg:
        print(f"  ERROR: Connection refused at {endpoint}")
        print("         Is the OneBucket endpoint reachable from this machine?")
    elif "name or service not known" in msg or "nodename nor servname" in msg or "getaddrinfo" in msg:
        print(f"  ERROR: Cannot resolve hostname in {endpoint}")
        print("         Check ONEBUCKET_ENDPOINT and your DNS / network settings.")
    elif "timed out" in msg or "timeout" in msg:
        print(f"  ERROR: Connection timed out to {endpoint}")
        print("         The endpoint may be unreachable or slow to respond.")
    else:
        print(f"  ERROR: {type(exc).__name__}: {exc}")


def run_check() -> None:
    print("=" * 60)
    print("  OneBucket connectivity check")
    print("=" * 60)

    endpoint_raw = os.environ.get("ONEBUCKET_ENDPOINT", "")
    access_key = os.environ.get("ONEBUCKET_ACCESS_KEY_ID", "")
    secret_key = os.environ.get("ONEBUCKET_SECRET_ACCESS_KEY", "")
    bucket = os.environ.get("ONEBUCKET_BUCKET", "")
    region = os.environ.get("ONEBUCKET_REGION", "us-east-1")
    force_path_style = os.environ.get("ONEBUCKET_FORCE_PATH_STYLE", "true").lower() == "true"
    skip_verify = os.environ.get("ONEBUCKET_SKIP_VERIFY", "false").lower() == "true"

    print(f"\n  Endpoint:         {endpoint_raw or '(not set)'}")
    print(f"  Access key:       {redact(access_key)}")
    print(f"  Secret key:       {redact(secret_key)}")
    print(f"  Bucket:           {bucket or '(not set)'}")
    print(f"  Region:           {region or '(not set)'}")
    print(f"  Path-style:       {force_path_style}")
    print(f"  Skip TLS verify:  {skip_verify}")
    print()

    # Validate required fields
    errors: list[str] = []
    if not access_key:
        errors.append("ONEBUCKET_ACCESS_KEY_ID is not set")
    elif access_key != access_key.strip():
        errors.append("ONEBUCKET_ACCESS_KEY_ID has leading/trailing whitespace — check your .env")
    if not secret_key:
        errors.append("ONEBUCKET_SECRET_ACCESS_KEY is not set")
    elif secret_key != secret_key.strip():
        errors.append("ONEBUCKET_SECRET_ACCESS_KEY has leading/trailing whitespace — check your .env")
    if not bucket:
        errors.append("ONEBUCKET_BUCKET is not set")

    try:
        endpoint = validate_endpoint(endpoint_raw)
    except ValueError as exc:
        errors.append(str(exc))
        endpoint = ""

    if errors:
        print("Configuration errors:")
        for err in errors:
            print(f"  ERROR: {err}")
        sys.exit(1)

    # Build client
    print("Building S3 client...")
    try:
        client = make_s3_client(endpoint, access_key, secret_key, region, force_path_style, skip_verify)
    except Exception as exc:
        print(f"  ERROR: Failed to create S3 client: {exc}")
        sys.exit(1)
    print("  OK\n")

    print(f"Testing bucket: s3://{bucket}/")

    # 1. Head bucket
    print("  [1/4] Checking bucket existence (HeadBucket)...")
    try:
        client.head_bucket(Bucket=bucket)
        print("        OK")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("403", "AccessDenied"):
            print(f"        ERROR: Access denied to bucket '{bucket}'.")
            print("               Verify credentials have s3:HeadBucket / s3:GetBucketLocation permission.")
        elif code in ("404", "NoSuchBucket"):
            print(f"        ERROR: Bucket '{bucket}' does not exist.")
            print("               Check ONEBUCKET_BUCKET.")
        elif code == "301":
            print(f"        ERROR: Permanent redirect — region may be wrong (current: {region}).")
        else:
            print(f"        ERROR: {code} — {exc.response['Error'].get('Message', str(exc))}")
        sys.exit(1)
    except Exception as exc:
        print()
        diagnose_connection_error(exc, endpoint, skip_verify)
        sys.exit(1)

    # 2. Write test object
    print(f"  [2/4] Writing test object ({TEST_KEY})...")
    try:
        client.put_object(Bucket=bucket, Key=TEST_KEY, Body=TEST_BODY)
        print("        OK")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        print(f"        ERROR: Write failed: {code} — {exc.response['Error'].get('Message', str(exc))}")
        print("               Verify credentials have s3:PutObject permission.")
        sys.exit(1)

    # 3. Read test object
    print(f"  [3/4] Reading test object ({TEST_KEY})...")
    try:
        resp = client.get_object(Bucket=bucket, Key=TEST_KEY)
        body = resp["Body"].read()
        if body != TEST_BODY:
            print(f"        ERROR: Content mismatch. Expected {TEST_BODY!r}, got {body!r}")
            sys.exit(1)
        print("        OK")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        print(f"        ERROR: Read failed: {code} — {exc.response['Error'].get('Message', str(exc))}")
        sys.exit(1)

    # 4. Delete test object
    print(f"  [4/4] Deleting test object ({TEST_KEY})...")
    try:
        client.delete_object(Bucket=bucket, Key=TEST_KEY)
        print("        OK")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        print(f"        WARNING: Delete failed: {code} — manual cleanup may be needed under {TEST_PREFIX}")

    print()
    print("=" * 60)
    print("  OneBucket connectivity check PASSED")
    print("=" * 60)
    print()
    print("Next step: make up")


if __name__ == "__main__":
    run_check()
