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

### Step 3 — Configure FSx Security Group and NLB

**Security group** (allows EC2 → ONTAP traffic):

1. Go to **EC2 → Security Groups**
2. Find the FSx security group (the one attached to your FSx filesystem)
3. **Edit inbound rules → Add rule:**
   - Type: All traffic
   - Source: `lakefs-ontap-demo-sg`
4. Save rules

**Network Load Balancer** (resolves ONTAP S3 pre-signed URLs — needed for the
`everest mount` step in Part 2):

5. **Edit inbound rules → Add another rule:**
   - Type: TCP, Port: 80
   - Source: **My IP**
   - Description: `ONTAP S3 HTTP via NLB for pre-signed URLs`

   > ⚠️ **Scope this narrowly.** ONTAP S3 here runs over plaintext HTTP, so S3
   > traffic and pre-signed URL signatures travel unencrypted. Allow only the
   > addresses that actually need to resolve pre-signed URLs — for this guide
   > that is your own IP. **Never use `0.0.0.0/0`:** it publishes an
   > unencrypted storage endpoint to the internet, where the only thing standing
   > between a passer-by and your bucket is the ONTAP S3 access key. For any
   > real use, enable HTTPS on the object-store server and front the NLB with TLS.
6. Go to **EC2 → Load Balancers → Create load balancer → Network Load Balancer**
   - Name: `lakefs-ontap-s3`
   - Scheme: **Internet-facing**
   - Listener: TCP port 80
   - Target group: IP type, TCP port 80, target = the SVM management IP (from Step 4)
7. Note the NLB DNS name — you'll need it for the lakeFS config (`pre_signed_endpoint`)

> **Terraform users:** everything in this step is handled by `terraform apply`.
> The port-80 source defaults to your current public IP; override it with
> `ontap_s3_allowed_cidrs` in `terraform.tfvars` only if you need to.

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

> This demo uses **lakeFS Enterprise** because the `everest mount` workflow relies on pre-signed URLs resolving from outside the VPC (the reason for the NLB in Step 3), and mount is an Enterprise feature. You'll need a license token. (lakeFS OSS also works against ONTAP for the API-only flow — see the footnote at the end of this step.)

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

# Install lakeFS Enterprise.
# Set this to the version your account team provided, or the latest listed at
# https://docs.lakefs.io/enterprise/
LAKEFS_VERSION=<LAKEFS_ENTERPRISE_VERSION>
curl -L "https://artifacts.lakefs.io/lakefs-enterprise/${LAKEFS_VERSION}/lakefs-enterprise_${LAKEFS_VERSION}_Linux_x86_64.tar.gz" | tar xz
sudo mv lakefs /usr/local/bin/
```

Get the SVM management IP from FSx console → Storage Virtual Machines → fsx → Endpoints → Management IP address.
Get the NLB DNS from `terraform output ontap_s3_endpoint` or the AWS console.

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
    pre_signed_endpoint: "http://REPLACE_WITH_NLB_DNS"
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

> **Footnote — running lakeFS OSS instead.** If you only need the API-only flow (repos, branches, commits, content-addressed blocks on ONTAP) and don't need `everest mount`, you can run lakeFS OSS instead — no license token, and the NLB / `pre_signed_endpoint` plumbing is unnecessary. Swap the Enterprise install for the OSS binary:
> ```bash
> LAKEFS_VERSION=$(curl -s https://api.github.com/repos/treeverse/lakeFS/releases/latest | grep '"tag_name"' | cut -d'"' -f4 | sed 's/v//')
> curl -L "https://github.com/treeverse/lakeFS/releases/download/v${LAKEFS_VERSION}/lakeFS_${LAKEFS_VERSION}_Linux_x86_64.tar.gz" | tar xz
> sudo mv lakefs /usr/local/bin/
> ```
> Then use the same `~/lakefs.yaml` above, minus the `pre_signed_endpoint`, `license`, `features`, and `auth.ui_config` keys.
>
> `scripts/setup-demo.sh` automates exactly this OSS path (install, Postgres,
> config, start) if you'd rather not do it by hand:
> ```bash
> bash setup-demo.sh <SVM_MANAGEMENT_IP> <ONTAP_ACCESS_KEY> <ONTAP_SECRET_KEY>
> ```

---

### Step 6 — Complete lakeFS Setup

1. Open browser: `http://<EC2_PUBLIC_IP>:8000/setup`
2. Click through setup — it will generate an **Access Key ID** and **Secret Key**
3. **Save these credentials** — you'll need them to run the demo

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

This is the payoff for the Enterprise setup: mounting a lakeFS path as a local filesystem. `everest` streams objects on demand via pre-signed URLs — which is why Step 3's NLB and the `pre_signed_endpoint` config are required.

On EC2, install the `everest` binary (ships with lakeFS Enterprise):
```bash
# Set this to the everest version matching your lakeFS Enterprise release
# (your account team provides it, or see https://docs.lakefs.io/enterprise/)
EVEREST_VERSION=<EVEREST_VERSION>
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

> If `everest mount` hangs or errors on fetch, the pre-signed URLs aren't reachable — confirm `pre_signed_endpoint` points at the NLB DNS and that the NLB target (the SVM management IP) is healthy.

### Inspect the raw S3 objects on ONTAP (optional):

From EC2, install AWS CLI if not already installed:
```bash
pip3 install awscli --upgrade
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

**Terraform users:** `terraform destroy` removes everything it created. Verify
afterwards that the FSx filesystem is really gone — FSx keeps billing until it is.

**Manual teardown — delete in this order:**

1. **Network Load Balancer:** EC2 → Load Balancers → `lakefs-ontap-s3` → Delete,
   then delete its target group. Do this first — it is what makes the ONTAP S3
   endpoint reachable from outside the VPC.
2. **FSx Volume:** FSx → lakefs-ontap-demo → Volumes → vol1 → Delete
3. **FSx SVM:** FSx → lakefs-ontap-demo → Storage Virtual Machines → fsx → Delete
4. **FSx Filesystem:** FSx → lakefs-ontap-demo → Actions → Delete
5. **EC2:** EC2 → Instances → lakefs-ontap-demo → Terminate
6. **Elastic IP:** EC2 → Elastic IPs → Release (otherwise you're charged for unused EIP)

> **Teardown is what revokes the ONTAP S3 credentials.** Those keys live in the
> SVM, so deleting the SVM (step 3) destroys the `lakefs` user and its key pair.
> Until then the keys remain valid. If you need to revoke them while keeping the
> filesystem, SSH to ONTAP and run
> `vserver object-store-server user delete -vserver fsx -user lakefs`.

> **Confirm the filesystem is deleted.** An FSx ONTAP filesystem left running
> costs ~$8.40/day indefinitely and keeps its S3 endpoint live. Check with:
> ```bash
> aws fsx describe-file-systems --query 'FileSystems[].{Id:FileSystemId,State:Lifecycle}'
> ```

---

## PART 4 — Costs Reference

| Resource | Cost |
|----------|------|
| FSx ONTAP (1TB, 384MB/s) | ~$8.40/day |
| EC2 t3.small | ~$0.50/day |
| Network Load Balancer | ~$0.55/day + data processing |
| Elastic IP (when attached) | Free |
| **Total per demo day** | **~$9.50/day** |

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
| `everest mount` hangs on fetch | Pre-signed URLs aren't resolving — check `pre_signed_endpoint` points at the NLB DNS, the NLB target is healthy, and your IP is in `ontap_s3_allowed_cidrs` |
