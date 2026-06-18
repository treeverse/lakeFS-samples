# lakeFS + NetApp ONTAP Demo — Setup Guide

## Overview

This guide sets up a live demo of lakeFS running against a native NetApp ONTAP S3 bucket on AWS FSx.

**Cost:** ~$8/day while running. **Tear it down after the demo** — total cost per demo ~$10.

**Time to set up:** ~35 minutes (mostly waiting for FSx to provision)

---

## Prerequisites

- AWS account access (us-east-1)
- The `lakefs-ontap-demo.pem` key file
- A terminal (Mac: Terminal app)

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
   - VPC: `demo-lakefs-vpc`
   - Subnet: `demo-lakefs-ontap-public` (`subnet-xxxxxxxxxxxxxxxxx`)
   - VPC Security Groups: leave default
   - File system admin password: `Netapp1!`
   - SVM name: `fsx`
   - SVM admin password: `Netapp1!`
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
   - Key pair: `lakefs-ontap-demo`
   - VPC: `demo-lakefs-vpc`
   - Subnet: `demo-lakefs-ontap-public`
   - Auto-assign public IP: **Enable**
   - Security group: select existing `lakefs-ontap-demo-sg`
     - (If it doesn't exist, create new with: SSH/22, TCP/8000, HTTPS/443 — all **My IP**)
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

**Network Load Balancer** (exposes ONTAP S3 publicly for pre-signed URL support):

5. **Edit inbound rules → Add another rule:**
   - Type: TCP, Port: 80
   - Source: `0.0.0.0/0`
   - Description: `ONTAP S3 HTTP via NLB for pre-signed URLs`

   > ⚠️ **PoC only.** This exposes the ONTAP S3 endpoint to the public internet over plaintext HTTP — pre-signed URL signatures and S3 traffic travel unencrypted. For anything beyond a demo, scope the source to your own IP and front the NLB with TLS.
6. Go to **EC2 → Load Balancers → Create load balancer → Network Load Balancer**
   - Name: `lakefs-ontap-s3`
   - Scheme: **Internet-facing**
   - Listener: TCP port 80
   - Target group: IP type, TCP port 80, target = the SVM management IP (from Step 4)
7. Note the NLB DNS name — you'll need it for the lakeFS config (`pre_signed_endpoint`)

> **Terraform users:** Steps 3–7 are handled automatically by `terraform apply`.

---

### Step 4 — Configure ONTAP S3

Once FSx shows **Available**:

1. Go to **FSx → lakefs-ontap-demo → Administration tab**
2. Note the **Management endpoint IP address** — this is your `<FSX_MANAGEMENT_IP>`

3. SSH into EC2:
```bash
ssh -i ~/path/to/lakefs-ontap-demo.pem ubuntu@<EC2_PUBLIC_IP>
```

4. From EC2, SSH into ONTAP:
```bash
ssh fsxadmin@<FSX_MANAGEMENT_IP>
# Password: Netapp1!
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
⚠️ **IMPORTANT: Copy the Access Key and Secret Key printed here — you won't see them again!**

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

First, copy the lakeFS Enterprise license file from your machine to EC2:
```bash
scp -i ~/.ssh/lakefs-ontap-demo.pem license.token ubuntu@<EC2_PUBLIC_IP>:/home/ubuntu/
```


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

# Install lakeFS Enterprise
LAKEFS_VERSION=REPLACE_WITH_LAKEFS_ENTERPRISE_VERSION_NO
curl -L "https://artifacts.lakefs.io/lakefs-enterprise/${LAKEFS_VERSION}/lakefs-enterprise_${LAKEFS_VERSION}_Linux_x86_64.tar.gz" | 
tar xz
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
ssh -i ~/path/to/lakefs-ontap-demo.pem ubuntu@<EC2_PUBLIC_IP>
curl -s http://localhost:8000/api/v1/healthcheck
```

If no response, restart lakeFS:
```bash
nohup lakefs run --config ~/lakefs.yaml > ~/lakefs.log 2>&1 &
```

### Run the demo script:
```bash
cd ~/demo/demo
LAKEFS_ACCESS_KEY_ID=<YOUR_KEY> \
LAKEFS_SECRET_ACCESS_KEY=<YOUR_SECRET> \
python3 demo_flow.py
```

> If it fails with "storage namespace already in use", the script will auto-increment the version (v2, v3, etc.)

### Open these browser tabs:
1. `http://<EC2_PUBLIC_IP>:8000/repositories` — lakeFS UI
2. AWS Console → FSx → `lakefs-ontap-demo` — ONTAP storage view

### Mount the dataset as files (lakeFS Enterprise — `everest mount`):

This is the payoff for the Enterprise setup: mounting a lakeFS path as a local filesystem. `everest` streams objects on demand via pre-signed URLs — which is why Step 3's NLB and the `pre_signed_endpoint` config are required.

On EC2, install the `everest` binary (ships with lakeFS Enterprise):
```bash
EVEREST_VERSION=REPLACE_WITH_EVEREST_VERSION_NO
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

### To show raw S3 objects on ONTAP (optional, great for technical audiences):

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

**Delete in this order:**

1. **FSx Volume:** FSx → lakefs-ontap-demo → Volumes → vol1 → Delete
2. **FSx SVM:** FSx → lakefs-ontap-demo → Storage Virtual Machines → fsx → Delete
3. **FSx Filesystem:** FSx → lakefs-ontap-demo → Actions → Delete
4. **EC2:** EC2 → Instances → lakefs-ontap-demo → Terminate
5. **Elastic IP:** EC2 → Elastic IPs → Release (otherwise you're charged for unused EIP)

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
| Can't SSH to ONTAP | Must SSH from EC2 (private IP), not from your Mac |
| ONTAP password rejected | Password is `Netapp1!` for both fsxadmin and vsadmin |
