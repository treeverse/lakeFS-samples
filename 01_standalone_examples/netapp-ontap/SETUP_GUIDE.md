# lakeFS + NetApp ONTAP Demo — Setup Guide

## Overview

This guide sets up a live demo of lakeFS running against a native NetApp ONTAP S3 bucket on AWS FSx.

**Cost:** ~$9/day while running. **Tear it down after the demo** — total cost per demo ~$10.

**Time to set up:** ~30 minutes (mostly waiting for FSx to provision)

---

## Prerequisites

- AWS account access (this guide assumes `us-east-1`)
- An existing EC2 key pair in that account/region, and its `.pem` file locally
- A VPC with a public subnet
- A terminal

Throughout this guide, replace the placeholders in `<ANGLE_BRACKETS>` with your
own values. Resource names like `lakefs-ontap-demo` are suggestions — use
whatever you prefer, as long as you stay consistent.

---

## PART 1 — Build the Infrastructure (30 min, mostly waiting)

### Step 1 — Create FSx for NetApp ONTAP

1. Go to **AWS Console → FSx → Create file system**
2. Select **Amazon FSx for NetApp ONTAP** → Next
3. Select **Standard create**
4. Fill in:
   - File system name: `lakefs-ontap-demo`
   - Deployment type: **Single-AZ 2**
   - SSD storage: `1024` GiB
   - Throughput: **Recommended (384 MB/s)**
   - VPC: `<YOUR_VPC>`
   - Subnet: `<YOUR_PUBLIC_SUBNET>`
   - VPC Security Groups: leave default
   - File system admin password: **choose your own** — this is the `fsxadmin`
     storage-admin account (min 8 chars, mixed case + number/special)
   - SVM name: `fsx`
   - SVM admin password: **choose your own** — the `vsadmin` account
   - Storage efficiency: **Enabled**
   - Everything else: leave as default
5. Click **Create file system**
6. ⏳ Wait 20-30 minutes for status to show **Available**

> While waiting, continue with Steps 2 and 3.

---

### Step 2 — Launch EC2 Instance

1. Go to **AWS Console → EC2 → Launch Instance**
2. Fill in:
   - Name: `lakefs-ontap-demo`
   - AMI: **Ubuntu Server 22.04 LTS**
   - Instance type: **t3.small**
   - Key pair: `<YOUR_KEY_PAIR>`
   - VPC: `<YOUR_VPC>`
   - Subnet: `<YOUR_PUBLIC_SUBNET>`
   - Auto-assign public IP: **Enable**
   - Security group: create one named `lakefs-ontap-demo-sg` with
     SSH/22 and TCP/8000 scoped to **My IP**
3. Click **Launch instance**
4. Go to **EC2 → Elastic IPs → Allocate → Associate** to the new instance
   (This keeps the IP stable across stops/starts)

---

### Step 3 — Configure the FSx Security Group

**Security group** (allows EC2 → ONTAP traffic):

1. Go to **EC2 → Security Groups**
2. Find the FSx security group (the one attached to your FSx filesystem)
3. **Edit inbound rules → Add rule:**
   - Type: All traffic
   - Source: `lakefs-ontap-demo-sg`
4. Save rules

> **Terraform users:** this step is handled by `terraform apply`.
>
> **Note on network exposure.** ONTAP S3 here is reached only over the VPC's
> private network, from the EC2 host. Nothing about this demo requires exposing
> the ONTAP S3 endpoint to the internet, and you should not do so: it speaks
> plaintext HTTP, so the only thing protecting the bucket would be the S3 access
> key. If you ever need pre-signed URLs to resolve from outside the VPC, enable
> HTTPS on the object-store server and front it with TLS first.

---

### Step 4 — Configure ONTAP S3

Once FSx shows **Available**:

1. Go to **FSx → lakefs-ontap-demo → Administration tab**
2. Note the **Management endpoint IP address** — this is your `<FSX_MANAGEMENT_IP>`

3. SSH into EC2:
```bash
ssh -i ~/path/to/<YOUR_KEY_PAIR>.pem ubuntu@<EC2_PUBLIC_IP>
```

4. From EC2, SSH into ONTAP:
```bash
ssh fsxadmin@<FSX_MANAGEMENT_IP>
# Password: the fsxadmin password you chose in Step 1
```

5. Run these commands in order:

```
# Check if S3 server already exists
vserver object-store-server show -vserver fsx
```

If no S3 server exists, create one:
```
vserver object-store-server create -vserver fsx -object-store-server s3.demo -is-http-enabled true -is-https-enabled false
```

Create S3 user:
```
vserver object-store-server user create -vserver fsx -user lakefs
```
⚠️ **Copy the Access Key and Secret Key printed here — you won't see them again.**
ONTAP generates them randomly at user creation and never displays the secret
again; if you lose them, `user delete` then `user create` to get a fresh pair.
Treat them as secrets — they grant full access to the bucket. They belong in
`.env` / `lakefs.yaml` (both gitignored), never in a commit or a shared doc.

Create bucket:
```
vserver object-store-server bucket create -vserver fsx -bucket lakefs-data -size 500GB
```

Set bucket policy:
```
vserver object-store-server bucket policy statement create -vserver fsx -bucket lakefs-data -effect allow -action * -principal lakefs -resource lakefs-data,lakefs-data/*
```

Type `exit` to leave ONTAP CLI.

---
### Step 5 — Install and Configure lakeFS Enterprise on EC2

> This demo uses **lakeFS Enterprise**, because the `everest mount` workflow in Part 2 is an Enterprise feature. You'll need a license token; contact your Treeverse account team if you don't have one.

First, copy the lakeFS Enterprise license file from your machine to EC2. Your
Treeverse account team provides `license.token` along with the current Enterprise
version number:
```bash
scp -i ~/.ssh/<YOUR_KEY_PAIR>.pem license.token ubuntu@<EC2_PUBLIC_IP>:/home/ubuntu/
```

> `license.token` is a secret tied to your organization. This directory's
> `.gitignore` excludes it — keep it that way.

Still on EC2, run:

```bash
# Install Docker and Python
sudo apt-get update && sudo apt-get install -y docker.io python3-pip
sudo systemctl start docker
sudo usermod -aG docker ubuntu
newgrp docker

# Start Postgres
docker run -d --name postgres \
  -e POSTGRES_USER=lakefs \
  -e POSTGRES_PASSWORD=lakefs \
  -e POSTGRES_DB=lakefs \
  -p 5432:5432 \
  --restart unless-stopped \
  postgres:15-alpine

# Install lakeFS Enterprise. Verified with 1.92.0; check
# https://hub.docker.com/r/treeverse/lakefs-enterprise/tags for newer releases.
LAKEFS_VERSION=1.92.0
curl -L "https://artifacts.lakefs.io/lakefs-enterprise/${LAKEFS_VERSION}/lakefs-enterprise_${LAKEFS_VERSION}_Linux_x86_64.tar.gz" | tar xz
sudo mv lakefs /usr/local/bin/
```

Get the SVM management IP from FSx console → Storage Virtual Machines → fsx → Endpoints → Management IP address, or with `terraform output svm_management_ip`.

Create the config file (replace the values in CAPS). Generate the
`auth.encrypt.secret_key` with `openssl rand -hex 20`:
```bash
cat > ~/lakefs.yaml << 'EOF'
database:
  type: postgres
  postgres:
    connection_string: "postgresql://lakefs:lakefs@localhost:5432/lakefs?sslmode=disable"

auth:
  encrypt:
    secret_key: "REPLACE_WITH_RANDOM_HEX_SECRET"
  ui_config:
    rbac: internal

installation:
  user_name: admin
  access_key_id: "AKIAIOSFODNN7EXAMPLE"
  secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

blockstore:
  type: s3
  s3:
    endpoint: "http://REPLACE_WITH_SVM_MANAGEMENT_IP"
    force_path_style: true
    region: "us-east-1"
    credentials:
      access_key_id: "REPLACE_WITH_ONTAP_ACCESS_KEY"
      secret_access_key: "REPLACE_WITH_ONTAP_SECRET_KEY"

logging:
  level: "INFO"
  format: "text"

license:
  path: /home/ubuntu/license.token
features:
  local_rbac: true
EOF
```

Start lakeFS:
```bash
nohup lakefs run --config ~/lakefs.yaml > ~/lakefs.log 2>&1 &
```

Confirm it came up and is pointed at ONTAP:
```bash
curl -s http://localhost:8000/api/v1/healthcheck    # expect HTTP 204, no body
tail -20 ~/lakefs.log
```

---

### Step 6 — Complete lakeFS Setup

lakeFS starts uninitialized and has no users yet. The `installation` block in
the config above does **not** create the admin account on its own — until you
finish setup, every API call returns `401`.

1. Open browser: `http://<EC2_PUBLIC_IP>:8000/setup`
2. Enter `admin` as the username and complete setup
3. lakeFS shows an **Access Key ID** and **Secret Key**. **Save them** — the
   secret is displayed only once, and the demo needs both

The demo script defaults to the example credentials in `.env.example`
(`AKIAIOSFODNN7EXAMPLE` / `wJalrXUtnFEMI/...`). Setup generates different ones,
so pass the real values when you run it (shown in Part 2). If you would rather
keep the documented defaults so the demo runs with no arguments, initialize via
the API instead of the UI and supply them explicitly:

```bash
curl -X POST http://localhost:8000/api/v1/setup_lakefs \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","key":{
        "access_key_id":"AKIAIOSFODNN7EXAMPLE",
        "secret_access_key":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}}'
```

Verify setup landed and lakeFS is really pointed at ONTAP:
```bash
curl -s http://localhost:8000/api/v1/setup_lakefs        # expect "state":"initialized"
curl -s -u "<YOUR_KEY>:<YOUR_SECRET>" http://localhost:8000/api/v1/config
# storage_config.blockstore_type should be "s3"
```

---

## PART 2 — Run the Demo

### Before Every Demo Run

SSH into EC2 and make sure lakeFS is running:
```bash
ssh -i ~/path/to/<YOUR_KEY_PAIR>.pem ubuntu@<EC2_PUBLIC_IP>
curl -s http://localhost:8000/api/v1/healthcheck
```

If no response, restart lakeFS:
```bash
nohup lakefs run --config ~/lakefs.yaml > ~/lakefs.log 2>&1 &
```

### Run the demo script:

Copy the `demo/` directory to EC2 (from your machine):
```bash
scp -i ~/.ssh/<YOUR_KEY_PAIR>.pem -r demo ubuntu@<EC2_PUBLIC_IP>:/home/ubuntu/
```

Then on EC2:
```bash
cd ~/demo
pip3 install -r requirements.txt
LAKEFS_ACCESS_KEY_ID=<YOUR_KEY> \
LAKEFS_SECRET_ACCESS_KEY=<YOUR_SECRET> \
python3 demo_flow.py
```

> ⚠️ The script starts from a clean slate: it **deletes** any existing repository
> named `churn-features` on the target lakeFS before recreating it. Point it at a
> demo instance, not one holding data you care about.

> If it fails with "storage namespace already in use", the script will auto-increment the version (v2, v3, etc.)

### Open these browser tabs:
1. `http://<EC2_PUBLIC_IP>:8000/repositories` — lakeFS UI
2. AWS Console → FSx → `lakefs-ontap-demo` — ONTAP storage view

### Mount the dataset as files (lakeFS Enterprise — `everest mount`):

This is the payoff for the Enterprise setup: mounting a lakeFS path as a local filesystem, with objects fetched on demand rather than copied. `everest` reads them via pre-signed URLs, which lakeFS signs against the SVM endpoint — reachable directly from the EC2 host, so no extra networking is needed.

On EC2, install the `everest` binary (ships with lakeFS Enterprise):
```bash
# Verified with everest 0.11.0.
EVEREST_VERSION=0.11.0
curl -L "https://artifacts.lakefs.io/everest/${EVEREST_VERSION}/everest_${EVEREST_VERSION}_Linux_x86_64.tar.gz" | tar xz
sudo mv everest /usr/local/bin/
```

Point `everest` at lakeFS with the credentials from Step 6:
```bash
cat > ~/.lakectl.yaml << 'EOF'
server:
  endpoint_url: http://localhost:8000
credentials:
  access_key_id: <YOUR_KEY>
  secret_access_key: <YOUR_SECRET>
EOF
```

Mount the `main` branch of the demo repo as local files:
```bash
mkdir -p ~/churn-data
everest mount lakefs://churn-features/main/data ~/churn-data
```

Now browse the versioned dataset as if it were on disk — no copy, fetched on demand:
```bash
ls -l ~/churn-data
cat ~/churn-data/customers.csv
```

When done, unmount:
```bash
everest umount ~/churn-data
```

> If `everest mount` reports "timeout waiting for mount server", the pre-signed URLs aren't reachable from this host. Confirm `blockstore.s3.endpoint` is the SVM management IP and that `curl http://<SVM_MANAGEMENT_IP>/` from EC2 returns HTTP 403 (not a timeout). If you set a `pre_signed_endpoint`, make sure this host can actually reach it.

### Inspect the raw S3 objects on ONTAP (optional):

From EC2, install the AWS CLI if not already installed. `pip3` puts it in
`~/.local/bin`, which is not on `PATH` by default on Ubuntu 22.04:
```bash
pip3 install awscli --upgrade
export PATH=$PATH:~/.local/bin
```

Then list all objects lakeFS wrote to ONTAP S3:
```bash
AWS_ACCESS_KEY_ID=<ONTAP_ACCESS_KEY> \
AWS_SECRET_ACCESS_KEY="<ONTAP_SECRET_KEY>" \
AWS_DEFAULT_REGION=us-east-1 \
aws s3 ls s3://lakefs-data/ \
  --recursive \
  --endpoint-url http://<SVM_MANAGEMENT_IP>
```

This shows three levels of the stack:
- **lakeFS UI** → logical dataset with branches/commits
- **S3 objects** → content-addressed blocks on ONTAP
- **ONTAP CLI** (`volume show`) → physical FlexGroup volume (`fg_oss_*`) storing the data

---

## PART 3 — Tear Down (After Demo)

**Terraform users:** `terraform destroy` is **not** sufficient on its own — read
the warning at the end of this section first. The ONTAP S3 bucket's FlexGroup
volume is created outside Terraform and blocks SVM deletion.

**Manual teardown — delete in this order:**

1. **FSx Volume:** FSx → lakefs-ontap-demo → Volumes → vol1 → Delete
2. **FSx SVM:** FSx → lakefs-ontap-demo → Storage Virtual Machines → fsx → Delete
3. **FSx Filesystem:** FSx → lakefs-ontap-demo → Actions → Delete
4. **EC2:** EC2 → Instances → lakefs-ontap-demo → Terminate
5. **Elastic IP:** EC2 → Elastic IPs → Release (otherwise you're charged for unused EIP)

> **Teardown is what revokes the ONTAP S3 credentials.** Those keys live in the
> SVM, so deleting the SVM destroys the `lakefs` user and its key pair. Until
> then the keys remain valid. If you need to revoke them while keeping the
> filesystem, SSH to ONTAP and run
> `vserver object-store-server user delete -vserver fsx -user lakefs`.

### ⚠️ `terraform destroy` alone does not finish the job

Enabling ONTAP S3 in Step 4 makes ONTAP create its own FlexGroup volume to back
the bucket (`fg_oss_*`). Terraform never saw that volume, so it cannot delete it —
and FSx refuses to delete an SVM that still has non-root volumes. `terraform
destroy` therefore fails partway with:

```
Cannot delete storage virtual machine while it has non-root volumes: fsvol-…
```

**This leaves the filesystem running and billing.**

`aws fsx delete-volume` does *not* work on that volume — the request is accepted
and the volume briefly reports `DELETING`, then reverts to `CREATED`. The volume
belongs to the object-store server, so the only way to remove it is to delete the
**bucket**, from the ONTAP CLI. And ONTAP refuses to delete a bucket that still
has objects in it.

**So do this while the EC2 host still exists** — once it's gone you have nothing
left inside the VPC to reach ONTAP from, and you'll have to launch a throwaway
instance just to finish cleaning up.

On EC2, empty the bucket:
```bash
export PATH=$PATH:~/.local/bin
AWS_ACCESS_KEY_ID=<ONTAP_ACCESS_KEY> \
AWS_SECRET_ACCESS_KEY="<ONTAP_SECRET_KEY>" \
AWS_DEFAULT_REGION=us-east-1 \
aws s3 rm s3://lakefs-data/ --recursive --endpoint-url http://<SVM_MANAGEMENT_IP>
```

Then from the ONTAP CLI, delete the bucket (this drops the `fg_oss_*` FlexGroup):
```
ssh fsxadmin@<FSX_MANAGEMENT_IP>
vserver object-store-server bucket delete -vserver fsx -bucket lakefs-data
volume show -vserver fsx -fields size     # only fsx_root should remain
```

The FSx control plane lags behind ONTAP here: `describe-volumes` keeps listing the
`fg_oss_*` volume as `CREATED` for a while after ONTAP has dropped it, and SVM
deletion keeps failing until that record clears. Once the bucket is gone, this
call succeeds and clears it:
```bash
aws fsx delete-volume --volume-id <fsvol-...>   # now works; before, it reverted
```

Wait until `describe-volumes` lists only `fsx_root`, then run `terraform destroy`
(or the manual steps above) and it completes cleanly.

> **Always confirm the filesystem is really gone.** It costs ~$8.40/day for as
> long as it exists, whatever Terraform reported:
> ```bash
> aws fsx describe-file-systems \
>   --query 'FileSystems[].{Id:FileSystemId,State:Lifecycle}' --output table
> ```
> The list should not contain your filesystem. `terraform destroy` exiting
> non-zero is not a signal you can ignore.

---

## PART 4 — Costs Reference

| Resource | Cost |
|----------|------|
| FSx ONTAP (1TB, 384MB/s) | ~$8.40/day |
| EC2 t3.small | ~$0.50/day |
| Elastic IP (when attached) | Free |
| **Total per demo day** | **~$9/day** |

**Tip:** Spin up the morning of the demo, tear down the same evening = ~$10 total.

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| SSH times out | Your IP changed — update security group rules to **My IP** |
| lakeFS UI unreachable | SSH into EC2 and restart lakeFS (see above) |
| Demo script fails with "namespace in use" | Normal on re-run — script uses a new prefix automatically |
| Can't SSH to ONTAP | Must SSH from EC2 (the management IP is VPC-private), not from your laptop |
| ONTAP password rejected | Use the `fsxadmin` / `vsadmin` password you set in Step 1. Reset it via FSx → Actions → Update file system if needed |
| `everest mount` times out waiting for mount server | The host can't fetch objects. From EC2, `curl http://<SVM_MANAGEMENT_IP>/` should return HTTP 403. Also check `~/.everest` is owned by your user, not root |
