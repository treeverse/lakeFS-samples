"""`make deploy` -- provision the AWS AgentCore stack for the current run.

Creates, in order and recording each resource into run state for cleanup:

  1. AWS Secrets Manager secret with the lakeFS endpoint + keys (Lambda-only).
  2. Least-privilege IAM roles (Lambda execution, agent runtime).
  3. The curated-capabilities Lambda (packaged from this repo).
  4. The AgentCore Gateway (AWS_IAM inbound auth) + a Lambda target exposing the
     tool schemas.
  5. The AgentCore Policy engine + the rendered Cedar policies.
  6. The AgentCore Runtime agent (via the AgentCore CLI).

This requires live AWS credentials and the current AgentCore tooling. Control-
plane method names follow the AgentCore control-plane API; if your installed
boto3 / agentcore versions differ slightly, adjust the thin wrappers below. Every
created resource is recorded immediately, so `make cleanup` can tear down even a
partial deployment.

The model never receives lakeFS credentials: only the Lambda role can read the
secret, and the Lambda loads them server-side.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

from common.config import SAMPLE_ROOT, AWSConfig, LakeFSConfig
from common.runstate import RunState
from orchestrator.context import aws_session
from policy import authz

LAMBDA_PACKAGES = ["gateway", "common", "curation", "seed", "policy"]
TOOLS_SCHEMA = SAMPLE_ROOT / "gateway" / "schemas" / "tools.json"


def _prefix(aws_cfg: AWSConfig, rs: RunState) -> str:
    return f"{aws_cfg.resource_prefix}-{rs.run_id}"


def _strip_additional_properties(obj):
    """AgentCore inputSchema accepts only type/properties/required/items/description."""
    if isinstance(obj, dict):
        obj.pop("additionalProperties", None)
        for v in obj.values():
            _strip_additional_properties(v)
    elif isinstance(obj, list):
        for v in obj:
            _strip_additional_properties(v)
    return obj


# --- Lambda packaging -------------------------------------------------------
def build_lambda_zip(rs: RunState) -> Path:
    build_dir = SAMPLE_ROOT / "generated" / rs.run_id / "lambda-build"
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True)

    for pkg in LAMBDA_PACKAGES:
        shutil.copytree(SAMPLE_ROOT / pkg, build_dir / pkg)
    # Vendor the only runtime dependency the Lambda needs.
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--quiet", "requests", "-t", str(build_dir)],
        check=True,
    )

    zip_path = SAMPLE_ROOT / "generated" / rs.run_id / "lambda.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in build_dir.rglob("*"):
            if "__pycache__" in path.parts:
                continue
            if path.is_file():
                zf.write(path, path.relative_to(build_dir))
    return zip_path


# --- IAM --------------------------------------------------------------------
def _create_role(iam, name: str, trust: dict, inline: dict) -> str:
    """Create (or reuse) a role and (re)apply its inline policy. Idempotent."""
    try:
        role = iam.create_role(
            RoleName=name,
            AssumeRolePolicyDocument=json.dumps(trust),
            Description="agentcore-data-pr demo role",
        )
        arn = role["Role"]["Arn"]
    except iam.exceptions.EntityAlreadyExistsException:
        arn = iam.get_role(RoleName=name)["Role"]["Arn"]
    iam.put_role_policy(
        RoleName=name, PolicyName=f"{name}-inline", PolicyDocument=json.dumps(inline)
    )
    return arn


def _create_function_with_retry(lam, **kwargs):
    """create_function, retrying the IAM 'role cannot be assumed' propagation delay."""
    import time

    last = None
    for _attempt in range(12):
        try:
            return lam.create_function(**kwargs)
        except lam.exceptions.ResourceConflictException:
            # Function already exists (re-run): update its code + config.
            name = kwargs["FunctionName"]
            lam.update_function_code(FunctionName=name, ZipFile=kwargs["Code"]["ZipFile"])
            waiter = lam.get_waiter("function_updated")
            waiter.wait(FunctionName=name)
            lam.update_function_configuration(
                FunctionName=name,
                Environment=kwargs["Environment"],
                Timeout=kwargs["Timeout"],
                MemorySize=kwargs["MemorySize"],
                Role=kwargs["Role"],
                Handler=kwargs["Handler"],
            )
            return lam.get_function(FunctionName=name)["Configuration"]
        except lam.exceptions.InvalidParameterValueException as exc:
            if "cannot be assumed" not in str(exc):
                raise
            last = exc
            time.sleep(5)
    raise last


def _ensure_policy_engine(agentcore, name: str) -> tuple[str, str]:
    """Create (or reuse) a policy engine and wait until it is ready."""
    import time

    from botocore.exceptions import ClientError

    try:
        e = agentcore.create_policy_engine(name=name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConflictException":
            raise
        e = None
        for pe in agentcore.list_policy_engines(maxResults=100).get("policyEngines", []):
            if pe.get("name") == name:
                e = pe  # list items already carry policyEngineId + policyEngineArn
                break
        if e is None:
            raise
    engine_id = e["policyEngineId"]
    engine_arn = e["policyEngineArn"]
    for _ in range(40):
        pe = agentcore.get_policy_engine(policyEngineId=engine_id)
        if pe.get("status") in ("READY", "ACTIVE", "AVAILABLE"):
            break
        if pe.get("status") in ("FAILED", "DELETING"):
            raise RuntimeError(f"policy engine {engine_id} -> {pe.get('status')}")
        time.sleep(3)
    return engine_id, engine_arn


def _ensure_gateway(agentcore, name: str, role_arn: str, engine_arn: str) -> tuple[str, str]:
    """Create (or reuse) a gateway with the policy engine attached in ENFORCE mode."""
    import time

    from botocore.exceptions import ClientError

    last = None
    for _ in range(12):
        try:
            gw = agentcore.create_gateway(
                name=name,
                protocolType="MCP",
                authorizerType="AWS_IAM",
                roleArn=role_arn,
                policyEngineConfiguration={"arn": engine_arn, "mode": "ENFORCE"},
            )
            return gw["gatewayId"], gw["gatewayArn"]
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code == "ConflictException":
                for g in agentcore.list_gateways(maxResults=100).get("items", []):
                    if g.get("name") == name:
                        full = agentcore.get_gateway(gatewayIdentifier=g["gatewayId"])
                        return full["gatewayId"], full["gatewayArn"]
                raise
            # Newly-added IAM permissions (GetPolicyEngine/AuthorizeAction) may
            # still be propagating; retry these for a while.
            if code in ("ValidationException", "AccessDeniedException"):
                last = exc
                time.sleep(5)
                continue
            raise
    raise last


def _wait_gateway_ready(agentcore, gateway_id: str, timeout: int = 300) -> dict:
    import time

    for _ in range(timeout // 5):
        gw = agentcore.get_gateway(gatewayIdentifier=gateway_id)
        status = gw.get("status")
        if status == "READY":
            return gw
        if status in ("FAILED", "DELETING", "DELETED"):
            raise RuntimeError(f"gateway {gateway_id} -> {status}: {gw.get('statusReasons')}")
        time.sleep(5)
    raise TimeoutError(f"gateway {gateway_id} not READY after {timeout}s")


def deploy(rs: RunState) -> RunState:
    aws_cfg = AWSConfig.from_env()
    lakefs_cfg = LakeFSConfig.from_env()
    session = aws_session(aws_cfg)
    account = session.client("sts").get_caller_identity()["Account"]
    region = aws_cfg.region
    prefix = _prefix(aws_cfg, rs)

    sm = session.client("secretsmanager")
    iam = session.client("iam")
    lam = session.client("lambda")
    agentcore = session.client("bedrock-agentcore-control")

    # 1. Secret ------------------------------------------------------------
    secret_name = f"{prefix}-lakefs"
    secret_string = json.dumps(
        {
            "endpoint": lakefs_cfg.endpoint,
            "access_key_id": lakefs_cfg.access_key_id,
            "secret_access_key": lakefs_cfg.secret_access_key,
        }
    )
    try:
        secret = sm.create_secret(Name=secret_name, SecretString=secret_string)
        secret_arn = secret["ARN"]
    except sm.exceptions.ResourceExistsException:
        secret_arn = sm.describe_secret(SecretId=secret_name)["ARN"]
        sm.put_secret_value(SecretId=secret_arn, SecretString=secret_string)
    rs.record_aws_resource(type="secret", id=secret_arn, region=region)
    rs.save()

    # 2. IAM roles ---------------------------------------------------------
    lambda_trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    lambda_inline = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "arn:aws:logs:*:*:*",
            },
            {
                "Effect": "Allow",
                "Action": ["secretsmanager:GetSecretValue"],
                "Resource": secret_arn,
            },
        ],
    }
    lambda_role_arn = _create_role(
        iam, f"{prefix}-lambda", lambda_trust, lambda_inline
    )
    rs.record_aws_resource(type="iam_role", id=f"{prefix}-lambda", arn=lambda_role_arn)

    runtime_trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "bedrock-agentcore.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    runtime_inline = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Bedrock",
                "Effect": "Allow",
                "Action": ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
                "Resource": "*",
            },
            {
                # Gateway invocation + Cedar policy evaluation on every tool call.
                "Sid": "AgentCoreGatewayAndPolicy",
                "Effect": "Allow",
                "Action": [
                    "bedrock-agentcore:InvokeGateway",
                    "bedrock-agentcore:GetPolicyEngine",
                    "bedrock-agentcore:GetPolicy",
                    "bedrock-agentcore:ListPolicies",
                    "bedrock-agentcore:AuthorizeAction",
                    "bedrock-agentcore:PartiallyAuthorizeActions",
                    "bedrock-agentcore:BatchAuthorizeActions",
                ],
                "Resource": "*",
            },
            {
                # The AgentCore Runtime harness needs workload identity + memory.
                "Sid": "AgentCoreRuntime",
                "Effect": "Allow",
                "Action": [
                    "bedrock-agentcore:GetWorkloadAccessToken",
                    "bedrock-agentcore:GetWorkloadAccessTokenForJWT",
                    "bedrock-agentcore:GetWorkloadAccessTokenForUserId",
                    "bedrock-agentcore:CreateEvent",
                    "bedrock-agentcore:ListEvents",
                    "bedrock-agentcore:GetEvent",
                    "bedrock-agentcore:ListSessions",
                    "bedrock-agentcore:RetrieveMemoryRecords",
                    "bedrock-agentcore:ListMemoryRecords",
                ],
                "Resource": "*",
            },
            {
                "Sid": "Observability",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents",
                    "logs:DescribeLogStreams", "logs:DescribeLogGroups",
                    "xray:PutTraceSegments", "xray:PutTelemetryRecords",
                    "xray:GetSamplingRules", "xray:GetSamplingTargets",
                    "cloudwatch:PutMetricData",
                ],
                "Resource": "*",
            },
            {
                # The gateway invokes the capability Lambda using this role.
                "Sid": "InvokeCapabilityLambda",
                "Effect": "Allow",
                "Action": ["lambda:InvokeFunction"],
                "Resource": f"arn:aws:lambda:{region}:{account}:function:{prefix}-capabilities",
            },
        ],
    }
    runtime_role_arn = _create_role(
        iam, f"{prefix}-agent", runtime_trust, runtime_inline
    )
    rs.record_aws_resource(type="iam_role", id=f"{prefix}-agent", arn=runtime_role_arn)
    rs.save()

    # 3. Lambda ------------------------------------------------------------
    zip_path = build_lambda_zip(rs)
    fn = _create_function_with_retry(
        lam,
        FunctionName=f"{prefix}-capabilities",
        Runtime="python3.11",
        Role=lambda_role_arn,
        Handler="gateway.handler.lambda_handler",
        Code={"ZipFile": zip_path.read_bytes()},
        Timeout=60,
        MemorySize=256,
        Environment={
            "Variables": {
                "RUN_ID": rs.run_id,
                "LAKEFS_REPOSITORY": rs.repository,
                "SOURCE_BRANCH": rs.source_branch,
                "SOURCE_COMMIT": rs.source_commit or "",
                "BASELINE_COMMIT": rs.baseline_commit or "",
                "LAKEFS_SECRET_ARN": secret_arn,
                "AGENT_TASK": "",
            }
        },
    )
    lambda_arn = fn["FunctionArn"]
    rs.record_aws_resource(type="lambda", id=f"{prefix}-capabilities", arn=lambda_arn)
    rs.save()

    # 4. Policy engine (created BEFORE the gateway so it can be attached) --
    # Policy engine names allow only letters/digits/underscores (no dashes).
    engine_name = f"{prefix}-policy".replace("-", "_")
    engine_id, engine_arn = _ensure_policy_engine(agentcore, engine_name)
    rs.record_aws_resource(type="policy_engine", id=engine_id, arn=engine_arn)
    rs.save()

    # 5. Gateway (AWS_IAM inbound) with the policy engine attached in ENFORCE
    gateway_id, gateway_arn = _ensure_gateway(
        agentcore, f"{prefix}-gw", runtime_role_arn, engine_arn
    )
    gw_ready = _wait_gateway_ready(agentcore, gateway_id)
    gateway_arn = gw_ready["gatewayArn"]
    gateway_url = gw_ready["gatewayUrl"]
    rs.gateway_id = gateway_id
    rs.gateway_url = gateway_url
    rs.record_aws_resource(type="gateway", id=gateway_id, arn=gateway_arn)
    rs.save()

    # 6. Lambda target exposing the curated tool schema -------------------
    target_name = "lakefs-mcp-target"
    tool_schema = _strip_additional_properties(json.loads(TOOLS_SCHEMA.read_text()))
    import time as _time

    from botocore.exceptions import ClientError

    target_id = target_name
    last_exc = None
    for _ in range(12):
        try:
            tgt = agentcore.create_gateway_target(
                gatewayIdentifier=gateway_id,
                name=target_name,
                targetConfiguration={
                    "mcp": {
                        "lambda": {
                            "lambdaArn": lambda_arn,
                            "toolSchema": {"inlinePayload": tool_schema},
                        }
                    }
                },
                credentialProviderConfigurations=[
                    {"credentialProviderType": "GATEWAY_IAM_ROLE"}
                ],
            )
            target_id = tgt.get("targetId", target_name)
            break
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code == "ConflictException":
                break  # target already exists (re-run)
            if code == "ValidationException":  # role permission still propagating
                last_exc = exc
                _time.sleep(5)
                continue
            raise
    else:
        raise last_exc
    rs.record_aws_resource(type="gateway_target", id=target_id, gateway=gateway_id)
    try:
        lam.add_permission(
            FunctionName=f"{prefix}-capabilities",
            StatementId=f"{prefix}-gw-invoke",
            Action="lambda:InvokeFunction",
            Principal="bedrock-agentcore.amazonaws.com",
            SourceArn=gateway_arn,
        )
    except lam.exceptions.ResourceConflictException:
        pass  # permission already present (re-run)
    rs.save()

    # 7. Cedar policies: permit the curated tools, forbid merge -----------
    # AgentCore's IamEntity principal for an assumed role is the STS
    # assumed-role ARN, not the iam role ARN -- use that so `principal ==` matches.
    role_name = runtime_role_arn.split("/")[-1]
    agent_principal = f"arn:aws:sts::{account}:assumed-role/{role_name}"
    # AgentCore accepts one Cedar statement per policy, so deploy each
    # statement (permit, forbid) as its own policy in the engine.
    statements = authz.render_statements(
        agent_principal=agent_principal,
        gateway_arn=gateway_arn,
        target=target_name,
    )
    base = f"{prefix}-agent-policy".replace("-", "_")
    for i, statement in enumerate(statements):
        pname = f"{base}_{i}"
        try:
            policy = agentcore.create_policy(
                policyEngineId=engine_id,
                name=pname,
                definition={"cedar": {"statement": statement}},
                validationMode="IGNORE_ALL_FINDINGS",
            )
            pid = policy.get("policyId", pname)
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConflictException":
                raise
            pid = pname
        rs.record_aws_resource(type="policy", id=pid, engine=engine_id)
    rs.save()

    # 6. AgentCore Runtime (via the AgentCore CLI) ------------------------
    _deploy_runtime(rs, aws_cfg, prefix, gateway_url, runtime_role_arn)
    runtime_id, runtime_arn = _resolve_runtime(agentcore, prefix)
    if runtime_id:
        rs.runtime_arn = runtime_arn
        # Record the real agentRuntimeId -- `make cleanup` deletes by id, and the
        # resource prefix is not a valid one.
        rs.record_aws_resource(type="runtime", id=runtime_id, arn=runtime_arn)

    rs.stage = "deployed"
    rs.save()
    return rs


def _cli_env() -> dict:
    """Environment for the AgentCore CLI subprocess (ensures it is on PATH)."""
    import site

    env = dict(os.environ)
    user_bin = os.path.join(site.getuserbase(), "bin")
    env["PATH"] = user_bin + os.pathsep + env.get("PATH", "")
    env["AGENTCORE_SUPPRESS_RECOMMENDATION"] = "1"
    return env


def _runtime_agent_name(prefix: str) -> str:
    """The AgentCore agent name for a run. Letters/numbers/underscores only, <=48."""
    return f"{prefix}_agent".replace("-", "_")


def _resolve_runtime(agentcore, prefix: str) -> tuple[str | None, str | None]:
    """Look up the deployed runtime's real id + ARN from the control plane.

    The AgentCore CLI prints the ARN, but scraping it out of stdout is unreliable:
    the CLI wraps and decorates its output, which silently yields a truncated ARN
    and a runtime that `make cleanup` can then never delete. The control plane is
    authoritative, and the agent name is deterministic, so ask it directly.
    """
    name = _runtime_agent_name(prefix)
    try:
        runtimes = agentcore.list_agent_runtimes().get("agentRuntimes", [])
    except Exception as exc:  # noqa: BLE001
        print(f"WARNING: could not list AgentCore runtimes to record ids: {exc}")
        return None, None
    for rt in runtimes:
        if rt.get("agentRuntimeName") == name:
            return rt.get("agentRuntimeId"), rt.get("agentRuntimeArn")
    return None, None


def _deploy_runtime(
    rs: RunState, aws_cfg: AWSConfig, prefix: str, gateway_url: str, role_arn: str
) -> str | None:
    """Configure + deploy the agent on AgentCore Runtime via the AgentCore CLI.

    Uses direct_code_deploy (no Docker). The agent's runtime dependencies come
    from requirements.txt in the sample root.
    """
    env_vars = {
        "AGENTCORE_GATEWAY_URL": gateway_url or "",
        "BEDROCK_MODEL_ID": aws_cfg.model_id,
        "AWS_REGION": aws_cfg.region,
        "AGENT_TASK": "",
    }
    subenv = _cli_env()
    # AgentCore agent names allow only letters/numbers/underscores (<=48 chars).
    name = _runtime_agent_name(prefix)
    configure = [
        "agentcore", "configure",
        "-e", "agent/app.py",
        "-n", name,
        "-er", role_arn,
        "-r", aws_cfg.region,
        "-dt", "direct_code_deploy",
        "-rt", "PYTHON_3_11",
        "-p", "HTTP",
        "-ni",
    ]
    deploy_cmd = ["agentcore", "deploy"]
    for k, v in env_vars.items():
        deploy_cmd += ["-env", f"{k}={v}"]
    try:
        subprocess.run(configure, cwd=str(SAMPLE_ROOT), check=True, env=subenv)
        out = subprocess.run(
            deploy_cmd, cwd=str(SAMPLE_ROOT), check=True, capture_output=True,
            text=True, env=subenv,
        )
        blob = (out.stdout or "") + (out.stderr or "")
        for token in blob.split():
            if token.startswith("arn:aws:bedrock-agentcore") and "runtime" in token:
                return token.strip().rstrip(".,")
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        detail = ""
        if isinstance(exc, subprocess.CalledProcessError):
            detail = "\n" + (exc.stdout or "") + (exc.stderr or "")
        print(
            f"WARNING: AgentCore Runtime deploy step did not complete ({exc}).{detail}\n"
            "The Gateway, Lambda, and Policy are deployed. Ensure the `agentcore` "
            "CLI is installed and on PATH, or use `make run-local`."
        )
    return None


def main() -> int:
    rs = RunState.load_current()
    if rs.stage == "created":
        print("ERROR: run `make seed` before `make deploy`.")
        return 2
    print(f"Deploying AgentCore stack for run {rs.run_id}...\n")
    deploy(rs)
    print("\nDeployed. Gateway:", rs.gateway_id)
    print("Runtime:", rs.runtime_arn or "(not launched - see warning above)")
    print("\nNext: `make run` then `make test-policy-denial`.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
