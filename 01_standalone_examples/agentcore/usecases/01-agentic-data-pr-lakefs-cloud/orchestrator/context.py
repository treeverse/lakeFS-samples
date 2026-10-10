"""Connection helpers: lakeFS client and AWS session from configuration."""

from __future__ import annotations

from common.config import AWSConfig, LakeFSConfig
from gateway.lakefs_client import LakeFSClient


def lakefs_client_from_env() -> tuple[LakeFSClient, LakeFSConfig]:
    cfg = LakeFSConfig.from_env()
    client = LakeFSClient(cfg.endpoint, cfg.access_key_id, cfg.secret_access_key)
    return client, cfg


def aws_session(aws_cfg: AWSConfig | None = None):
    import boto3

    aws_cfg = aws_cfg or AWSConfig.from_env()
    kwargs = {"region_name": aws_cfg.region}
    if aws_cfg.profile:
        kwargs["profile_name"] = aws_cfg.profile
    return boto3.Session(**kwargs)
