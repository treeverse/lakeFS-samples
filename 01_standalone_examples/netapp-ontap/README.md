# lakeFS + NetApp ONTAP

Run **lakeFS data versioning backed by a native NetApp ONTAP S3 bucket** on
**AWS FSx for NetApp ONTAP**, then walk through a versioned-dataset workflow
end to end.

**Use case:** A data team adds a new ML feature (`payment_history_score`) to a
customer-churn dataset on a lakeFS branch, validates it, then merges to `main` —
all version-controlled in lakeFS, with the data physically stored on ONTAP S3.

The same lakeFS configuration applies to any ONTAP with S3 enabled — on-premises,
FSx for NetApp ONTAP, or Cloud Volumes ONTAP. This example uses FSx because it is
the quickest to stand up and tear down.

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
| `SETUP_GUIDE.md` | **The main runbook.** Full step-by-step: provision → enable ONTAP S3 → install lakeFS → run the demo → tear down |
| `terraform/` | Provisions the AWS infra: FSx ONTAP filesystem + SVM + volume, EC2, security groups, Elastic IP |
| `demo/` | The Python demo (`demo_flow.py`) and its seed data |
| `WALKTHROUGH.md` | What to look at in the lakeFS UI and the ONTAP CLI once the demo has run |
| `lakefs.yaml.example`, `.env.example` | Config templates (copy and fill in) |

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

Terraform outputs the EC2 IP, lakeFS UI URL, and the FSx/SVM management IPs. Then follow **`SETUP_GUIDE.md` from Step 4** to:

1. Enable the ONTAP S3 server and create the `lakefs` S3 user + `lakefs-data` bucket (SSH into ONTAP).
2. Install and configure lakeFS on the EC2 host (pointing its blockstore at the SVM management IP).
3. Open the lakeFS UI and run `demo/demo_flow.py`.

Then see [`WALKTHROUGH.md`](WALKTHROUGH.md) for what to look at across the three
layers — the lakeFS UI, the S3 objects, and the ONTAP volume underneath.

When you're done, **tear everything down** (`SETUP_GUIDE.md` Part 3) — the
environment costs roughly $9/day while running, and FSx keeps billing until the
filesystem is deleted.

> ⚠️ **`terraform destroy` does not complete on its own here.** Enabling ONTAP S3
> creates a FlexGroup volume outside Terraform's state, and FSx won't delete an SVM
> that still has one. Empty and delete the ONTAP S3 bucket **before** you tear down
> the EC2 host — it's the only thing that can reach the ONTAP CLI. See
> `SETUP_GUIDE.md` Part 3, then confirm with `aws fsx describe-file-systems` that
> nothing is left running.

---

## Security notes

This example is built for a short-lived demo environment. Before adapting it:

- **ONTAP S3 runs over plaintext HTTP** on port 80, to keep certificate handling
  out of the setup. It is reachable only from the EC2 host over the VPC's private
  network, never from the internet. Enable HTTPS on the object-store server
  before using this pattern for anything real.
- **Choose your own `fsxadmin` / `vsadmin` password.** These are full
  storage-admin accounts on the filesystem and SVM.
- **The ONTAP S3 access keys are shown once, at user creation.** They are what
  lakeFS uses to reach the bucket — treat them as secrets. `.env` and
  `lakefs.yaml` hold them and are both gitignored, along with `license.token`
  and `*.pem`.
- **The lakeFS admin credentials in the config templates** are AWS's published
  example values, used here so the demo is reproducible. Generate real ones via
  the lakeFS setup screen (`SETUP_GUIDE.md` Step 6) for anything you keep running.
- **Deleting the FSx filesystem deletes the SVM, and with it the ONTAP S3 user
  and its keys.** Tearing down is what revokes those credentials.
