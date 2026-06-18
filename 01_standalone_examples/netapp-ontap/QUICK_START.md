# ONTAP + lakeFS Demo — Quick Start

**Goal:** Run a live demo of lakeFS managing data on NetApp ONTAP S3.

**Use the AWS FSx for NetApp ONTAP path.** It is the supported path for this
PoC: provision with Terraform, then follow [SETUP_GUIDE.md](SETUP_GUIDE.md).

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # fill in your vpc/subnet/key/password
terraform init && terraform apply              # ~20–30 min
# then follow SETUP_GUIDE.md Steps 4–6
```

Cost: ~$8–10/day while running — **tear down after the demo**.
See **SETUP PATH 2** below for the summary and [SETUP_GUIDE.md](SETUP_GUIDE.md)
for the full runbook.

---

# SETUP PATH 1: Local UTM Demo — ⚠️ DEPRECATED (reference only)

> **Do not use this path.** Running the ONTAP simulator locally in UTM/VMware
> Fusion was abandoned — x86 emulation on Apple Silicon is too slow to be usable
> for a demo. It is kept here for reference only. Use the AWS FSx path above.

## Phase 1 — Prerequisites (15 min)

```bash
# Install tools
brew install --cask utm
brew install qemu
pip3 install requests

# Verify
/Applications/UTM.app/Contents/MacOS/utmctl --version
qemu-img --version | head -1
./scripts/check-prereqs.sh
```

## Phase 2 — Install lakeFS Binary (5 min)

```bash
cd ~/code/Ontap_lakeFS_Integration
bash scripts/install-lakefs.sh
./bin/lakefs --version  # expect: 1.80.0
```

## Phase 3 — Import ONTAP VM (30 min)

**Get the OVA file:**
- File: `~/Downloads/vsim-netapp-DOT9.18.1-cm_nodar.ova` (779 MB)
- If you don't have it, contact NetApp or use your existing copy

**Extract and convert:**
```bash
mkdir -p /tmp/ontap-ova
cd /tmp/ontap-ova
tar -xf ~/Downloads/vsim-netapp-DOT9.18.1-cm_nodar.ova

# Convert all 4 VMDKs to QCOW2 (takes ~3 minutes)
for vmdk in *.vmdk; do
  qemu-img convert -f vmdk -O qcow2 "$vmdk" "${vmdk%.vmdk}.qcow2"
done
ls -lh *.qcow2  # should see 4 files
```

**Create the VM in UTM:**
1. Open UTM → click **+**
2. Select **Emulate** (not Virtualize)
3. Operating System: **Other**
4. Architecture: **x86_64**
5. System: **Standard PC (Q35 + ICH9)**
6. RAM: **6144 MB** | CPU Cores: **2**
7. Storage: **uncheck** "Create a new image drive"
8. Name: `ONTAP-sim-9.18.1` → **Save** (don't start)
9. Edit VM → **Drives** → delete any auto-created drive
10. Click **New Drive → Import** → select each `.qcow2` in order (disk1–4)
    - Interface: **IDE** for each
11. Edit VM → **Network** → 
    - Mode: **Host Only** 
    - Card: **Intel Gigabit Ethernet (e1000)**
    - Save

**Boot the VM (10–15 min first boot):**

Start the VM. Wait for the cluster wizard. Enter:

```
Cluster name:                    ontap-sim
Node name:                       (press Enter for default)

Node management IP:              192.168.64.10
Cluster management IP:           192.168.64.11
Netmask:                         255.255.255.0
Gateway:                         192.168.64.1
DNS domain:                      lab.local
DNS servers:                     8.8.8.8
Admin password:                  Netapp1!
```

Wait for `ontap-sim::>` prompt.

**Verify from Mac:**
```bash
ping -c 3 192.168.64.11
ssh admin@192.168.64.11  # password: Netapp1!
```

## Phase 4 — Configure ONTAP S3 (10 min, inside ONTAP)

SSH into ONTAP:
```bash
ssh admin@192.168.64.11
```

Run these commands inside the ONTAP CLI:

```
# Check cluster
node show
storage aggregate show

# Create S3 SVM
vserver create -vserver s3svm -subtype default
vserver modify -vserver s3svm -allowed-protocols s3

# Create S3 data LIF
network interface create \
  -vserver s3svm \
  -lif s3_data_1 \
  -service-policy data-s3-server \
  -home-node ontap-sim-01 \
  -home-port e0c \
  -address 192.168.64.20 \
  -netmask 255.255.255.0 \
  -status-admin up

# Enable S3 server (HTTP only)
vserver object-store-server create \
  -vserver s3svm \
  -object-store-server s3.ontap.local \
  -is-http-enabled true \
  -is-https-enabled false \
  -port 80

# Create S3 user — COPY THE KEYS NOW
vserver object-store-server user create -vserver s3svm -user lakefs

# Create bucket
vserver object-store-server bucket create \
  -vserver s3svm \
  -bucket lakefs-data \
  -size 50GB

# Grant user access to bucket
vserver object-store-server bucket policy statement create \
  -vserver s3svm \
  -bucket lakefs-data \
  -effect allow \
  -action * \
  -principal lakefs \
  -resource lakefs-data,lakefs-data/*

exit
```

**Test S3 from Mac:**
```bash
curl -s http://192.168.64.20/lakefs-data/
# Should return XML or empty (not "connection refused")
```

## Phase 5 — Configure lakeFS (5 min)

```bash
cd ~/code/Ontap_lakeFS_Integration

# Copy config templates
cp .env.example .env
cp lakefs.yaml.example lakefs.yaml

# Generate secret key
openssl rand -hex 20
```

**Edit `.env`** — replace these values:
```bash
ONTAP_S3_ENDPOINT=http://192.168.64.20
ONTAP_S3_BUCKET=lakefs-data
ONTAP_S3_ACCESS_KEY=<from Phase 4 user creation>
ONTAP_S3_SECRET_KEY=<from Phase 4 user creation>

LAKEFS_AUTH_ENCRYPT_SECRET_KEY=<from openssl output>
LAKEFS_HOST=http://localhost:8000
```

**Edit `lakefs.yaml`** — replace these values:
```yaml
auth:
  encrypt:
    secret_key: "<from openssl output>"

blockstore:
  s3:
    endpoint: "http://192.168.64.20"
    credentials:
      access_key_id: "<ONTAP access key>"
      secret_access_key: "<ONTAP secret key>"
```

## Phase 6 — Start Services (5 min)

```bash
bash scripts/start-services.sh
```

This starts PostgreSQL + lakeFS. Verify:
```bash
bash scripts/verify-connectivity.sh
# All items should show [OK]
```

Check lakeFS is running:
```bash
curl -s http://localhost:8000/api/v1/healthcheck
```

## Phase 7 — Run the Demo (3 min)

```bash
source .env
python3 demo/demo_flow.py
```

You should see 11 steps printed. Open the UI:
```
http://localhost:8000
```

Login: Use the credentials you set in `.env` (default: `AKIAIOSFODNN7EXAMPLE`)

---

# SETUP PATH 2: AWS FSx Demo (For customer presentations)

See [SETUP_GUIDE.md](SETUP_GUIDE.md) for complete AWS setup instructions.

**Summary:**
1. Create FSx ONTAP filesystem (20–30 min)
2. Launch EC2 instance
3. Configure ONTAP S3 (10 min via SSH)
4. Install lakeFS on EC2 (5 min)
5. Run demo script (3 min)

Cost: ~$8–10/day. Tear down after demo.

---

# Common Tasks

## Run demo again
```bash
source .env && python3 demo/demo_flow.py
```

## Stop everything
```bash
bash scripts/stop-services.sh
```
(Then suspend the UTM VM)

## See lakeFS logs
```bash
tail -50 lakefs.log
```

## See PostgreSQL logs
```bash
docker logs postgres
```

## Restart services (if something breaks)
```bash
bash scripts/stop-services.sh
bash scripts/start-services.sh
```

## Check connectivity to ONTAP S3
```bash
source .env
curl -s http://$ONTAP_S3_ENDPOINT/$ONTAP_S3_BUCKET/
```

---

# Troubleshooting

| Issue | Fix |
|-------|-----|
| lakeFS won't start | Check `lakefs.log`. Verify `.env` and `lakefs.yaml` don't have `REPLACE_ME` values |
| Can't reach ONTAP S3 | Ping `192.168.64.20`. Is UTM VM running? Check lakeFS logs |
| HTTP 403 from S3 | Recreate ONTAP user: `ssh admin@192.168.64.11` → `vserver object-store-server user delete -vserver s3svm -user lakefs` → `vserver object-store-server user create -vserver s3svm -user lakefs` (copy new keys) |
| Port 8000 in use | `lsof -i :8000` to find process, or edit `lakefs.yaml` to use a different port |
| PostgreSQL won't start | `docker logs postgres` to see error. Check `/tmp` disk space |
| ONTAP VM very slow | Normal for x86 emulation. First boot takes 10–15 min. Be patient |

---

# What you end up with

```
Mac host
├── UTM VM (http://192.168.64.11)  →  ONTAP 9.18.1 Simulator
│                                      S3 endpoint: http://192.168.64.20
│                                      Bucket: lakefs-data
├── Docker (localhost:5432)  →  PostgreSQL
└── Binary (localhost:8000)  →  lakeFS
```

**Demo flow:**
1. Python script (`demo_flow.py`) talks to lakeFS API
2. lakeFS creates repo and branches
3. Data is stored on ONTAP S3 at `http://192.168.64.20`
4. You can visualize everything in the lakeFS UI

---

# For detailed info

- **Overview & architecture:** [README.md](README.md)
- **AWS setup (main runbook):** [SETUP_GUIDE.md](SETUP_GUIDE.md)
- **ONTAP CLI reference (deprecated simulator path):** [ontap-setup-notes.md](ontap-setup-notes.md)
- **Demo script source:** `demo/demo_flow.py`
