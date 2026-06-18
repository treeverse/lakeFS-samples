# lakeFS + NetApp ONTAP — Proof of Concept

Spin up **lakeFS data versioning backed by a native NetApp ONTAP S3 bucket** on
**AWS FSx for NetApp ONTAP**, then run a short demo end to end.

**Use case:** A data team adds a new ML feature (`payment_history_score`) to a
customer-churn dataset on a lakeFS branch, validates it, then merges to `main` —
all version-controlled in lakeFS, with the data physically stored on ONTAP S3.

---

## Architecture

```
AWS VPC
├── FSx for NetApp ONTAP   →  SVM "fsx"  →  native S3 object-store server (HTTP)
│                              Bucket: lakefs-data
├── EC2 (Ubuntu 22.04)     →  lakeFS (port 8000)  +  PostgreSQL (Docker, 5432)
└── Elastic IP             →  stable address for the lakeFS UI

Demo script (Python) → lakeFS API → ONTAP S3 blockstore
```

lakeFS uses ONTAP's **native S3** as its blockstore over **HTTP (port 80)**, with
`force_path_style: true` (ONTAP S3 does not support virtual-hosted-style URLs).

---

## What's in here

| Path | Purpose |
|------|---------|
| `terraform/` | Provisions the AWS infra: FSx ONTAP filesystem + SVM + volume, EC2, security groups, Elastic IP |
| `SETUP_GUIDE.md` | **The main runbook.** Full step-by-step: provision → enable ONTAP S3 → install lakeFS → run demo → tear down |
| `demo/` | The Python demo (`demo_flow.py`), seed data, and `DEMO_SCRIPT.md` talk track |
| `lakefs.yaml.example`, `.env.example` | Config templates (copy and fill in) |
| `scripts/` | Helper scripts (connectivity checks, service start/stop) |

> ⚠️ **Note on what Terraform does *not* do.** Terraform provisions the raw
> infrastructure only. Enabling the ONTAP S3 server, creating the S3 user/bucket,
> and installing lakeFS are **manual post-provisioning steps** done over SSH —
> see `SETUP_GUIDE.md` Steps 4–6. (AWS does not expose ONTAP S3 enablement
> through the FSx API.)

---

## Quickstart

Prerequisites: an AWS account (default region `us-east-1`), an existing EC2 key
pair, a VPC with a public subnet, and the AWS CLI + Terraform installed.

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars: set vpc_id, subnet_id, key_pair_name, fsxadmin_password

terraform init
terraform apply        # ~20–30 min, mostly waiting for FSx to provision
```

Terraform outputs the EC2 IP, lakeFS UI URL, and the FSx/SVM management IPs.
Then follow **`SETUP_GUIDE.md` from Step 4** to:

1. Enable the ONTAP S3 server and create the `lakefs` S3 user + `lakefs-data` bucket (SSH into ONTAP).
2. Install and configure lakeFS on the EC2 host (pointing its blockstore at the SVM management IP).
3. Open the lakeFS UI and run `demo/demo_flow.py`.

When you're done, **tear everything down** (`SETUP_GUIDE.md` Part 3) — the
environment costs ~$9/day while running.

---

## Deprecated: local ONTAP simulator path

An earlier approach ran the **ONTAP 9.18.1 simulator locally** in UTM/VMware
Fusion on a Mac (see `ontap-setup-notes.md`). **This was abandoned** — x86
emulation of the simulator on Apple Silicon is far too slow to be usable for a
demo. `ontap-setup-notes.md` is kept for reference only; **use the AWS FSx path
above.**
