# ONTAP Simulator Setup Notes
## NetApp ONTAP 9.18.1-CM (vsim) — Phase 1 Demo

> ⚠️ **DEPRECATED — reference only.** This local-simulator path (UTM/VMware
> Fusion on a Mac) was abandoned: x86 emulation of the ONTAP simulator on Apple
> Silicon is too slow to be usable for a demo. Use the **AWS FSx for NetApp
> ONTAP** path instead — see `README.md` and `SETUP_GUIDE.md`.

This document contains every command you need to run to get the ONTAP simulator
up and configured for the lakeFS demo. Keep this open while doing the setup.

---

## Part 1 — Install UTM

> VMware Fusion 13 on Apple Silicon **cannot run x86 VMs** (only ARM64 guests).
> UTM with QEMU emulation is the correct tool for this OVA on an M-series Mac.

---

### Option A — UTM (recommended if Broadcom portal is giving you trouble)

UTM is free, open source, and installs in one command:

```bash
brew install --cask utm
brew install qemu          # needed for VMDK → QCOW2 conversion
```

**Convert the OVA for UTM:**

```bash
mkdir -p /tmp/ontap-ova
cd /tmp/ontap-ova
tar -xf ~/Downloads/vsim-netapp-DOT9.18.1-cm_nodar.ova

# Convert all 4 VMDKs to QCOW2
for vmdk in *.vmdk; do
  echo "Converting $vmdk ..."
  qemu-img convert -f vmdk -O qcow2 "$vmdk" "${vmdk%.vmdk}.qcow2"
done
ls -lh *.qcow2
```

**Create the VM in UTM:**

1. UTM → **"+"** → **Emulate** (not Virtualize)
2. OS: Other | Architecture: **x86_64** | System: **Intel ICH9 based PC (Q35)**
3. RAM: **6144 MB** | CPU: **2** | Skip default storage disk
4. Name: `ONTAP-sim-9.18.1` → Save (don't start yet)
5. Edit VM → **Drives** tab → delete any auto-created disk
6. Add each qcow2 file in order (disk1 through disk4):
   - New Drive → Import → select `.qcow2` | Interface: **IDE**
7. Edit VM → **Network** tab:
   - Network Mode: **Host Only** (not Shared Network — Host Only lets your Mac reach the VM)
   - Host Network: **Default (private)**
   - Emulated card: **Intel Gigabit Ethernet (e1000)**

**UTM network addresses (use these in the cluster wizard):**

```
Node mgmt LIF:    10.0.2.10
Cluster mgmt LIF: 10.0.2.11
Default gateway:  10.0.2.2        ← UTM's virtual gateway
DNS:              8.8.8.8
S3 data LIF:      10.0.2.20       ← use in lakeFS config
```

> **Performance note:** UTM runs x86 via QEMU emulation on Apple Silicon.
> First boot takes 10–15 min. S3 API calls will be ~3× slower than native.
> Fine for a demo — just allow more time.

---

### Option B — VMware Fusion Pro (best performance, more complex to download)

VMware Fusion Pro is free since May 2024 but the Broadcom portal is awkward.

**If the downloads page is empty, try these fixes in order:**

1. After logging in, go to **My Dashboard → My Downloads**. If VMware Fusion
   is not listed, search the product catalogue for "VMware Fusion", open the
   product page, and click **"Register"** or **"Add to my account"**, then
   return to downloads.

2. Navigate via: `support.broadcom.com` → top menu **Software** → search
   "VMware Fusion" → product page → **Go to downloads** tab.

3. Try the **Mac App Store** — search "VMware Fusion". Same product, no
   Broadcom account needed.

**Once installed:**

1. File → Import → select `~/Downloads/vsim-netapp-DOT9.18.1-cm_nodar.ova`
2. Name it `ONTAP-sim-9.18.1`, click Continue through compatibility warning
3. VM Settings → Network Adapter → **Host-only** (vmnet1)
4. Check the hostonly subnet: VMware Fusion → Preferences → Network → vmnet1

**VMware Fusion hostonly network addresses:**

```
Mac gateway (vmnet1):  typically 192.168.233.1
Node mgmt LIF:         192.168.233.10
Cluster mgmt LIF:      192.168.233.11
S3 data LIF:           192.168.233.20    ← use in lakeFS config
```

> Substitute your actual vmnet1 subnet if it differs from 192.168.233.x.

---

## Part 2 — Import and Boot the OVA

### Import

1. Open VMware Fusion
2. File → Import…
3. Select: `~/Downloads/vsim-netapp-DOT9.18.1-cm_nodar.ova`
4. Name the VM: `ONTAP-sim-9.18.1`
5. VMware will warn about compatibility — click **Continue**
6. Save location: default is fine (~5 GB disk)

### Network adapter settings (CRITICAL)

The OVA comes with **hostonly** networking. You need the VM on a network that
your Mac host can reach. VMware Fusion hostonly is fine — keep it.

1. Before starting the VM: VM → Settings → Network Adapter
2. Confirm it is set to **Host-only** (vmnet1)
3. Note the hostonly subnet (usually shown as `192.168.x.0/24` or `172.16.x.0/24`)
   - You can check: VMware Fusion menu → Preferences → Network

The Mac host can always reach the hostonly network. Write down:

```
VMware Fusion hostonly subnet:  192.168.___.0/24
VMware Fusion gateway (Mac):    192.168.___.1   ← this is the Mac-side IP
```

### First boot and cluster wizard

1. Start the VM
2. Wait ~2 minutes for the ONTAP boot sequence (lots of output)
3. At the `cluster setup` prompt, follow the wizard:

```
Welcome to the cluster setup wizard.
...
Enter the cluster name: ontap-sim
Create a new cluster or join an existing cluster? [create/join]: create
Do you want to use IPv6? [n]: n
...
Enter the node management interface details
  IP address:     192.168.233.10    ← use an IP in your hostonly subnet
  Netmask:        255.255.255.0
  Default gateway: 192.168.233.1   ← the Mac-side IP of vmnet1
...
Enter the cluster management interface details
  IP address:     192.168.233.11    ← one more IP in the same subnet
  Netmask:        255.255.255.0
  Default gateway: 192.168.233.1
...
Enter DNS domain names []: lab.local
Enter DNS server IP addresses []: 8.8.8.8
...
Enter the cluster base license key: [leave blank for simulator — no license needed]
...
Enter new admin password:           Netapp1!
Retype new admin password:          Netapp1!
```

> **Note on IPs:** Replace `192.168.233.x` with addresses in YOUR hostonly
> subnet. The first host address (`.1`) is the Mac gateway. Use `.10` for node
> mgmt, `.11` for cluster mgmt, `.20` for the S3 data LIF.

4. Wait for cluster setup to complete (~3-5 minutes)
5. The shell prompt `ontap-sim::>` appears when ready

### Verify access from Mac

From your Mac terminal:
```bash
# Ping the node mgmt IP you configured
ping -c 3 192.168.233.10

# SSH in (accept fingerprint, password is what you set above)
ssh admin@192.168.233.11
```

---

## Part 3 — Configure ONTAP S3 (CLI commands)

All commands below are run **inside the ONTAP CLI** (either via SSH or the VM console).

SSH in:
```bash
ssh admin@192.168.233.11     # use your cluster mgmt IP
# password: Netapp1!  (or whatever you set)
```

### 3.1 — Survey the cluster

```ontap
# Check node name (you'll need it for LIF creation)
node show

# Check available aggregates (you need one for the SVM root volume)
storage aggregate show

# Check existing SVMs
vserver show
```

Expected: one node named `ontap-sim-01`, one or two aggregates (`aggr0`, `aggr1`).
Use `aggr1` for data (aggr0 is reserved for system).

### 3.2 — Create the S3 SVM

```ontap
# Create a new SVM dedicated to S3
vserver create -vserver s3svm -subtype default

# Assign S3 as the allowed protocol
vserver modify -vserver s3svm -allowed-protocols s3

# Verify
vserver show -vserver s3svm
```

### 3.3 — Create a data network interface (LIF) for S3

The data LIF is the IP address that lakeFS will use as the S3 endpoint.
Use an IP in your hostonly subnet (e.g. `.20`).

```ontap
network interface create \
  -vserver s3svm \
  -lif s3_data_1 \
  -service-policy data-s3-server \
  -home-node ontap-sim-01 \
  -home-port e0c \
  -address 192.168.233.20 \
  -netmask 255.255.255.0 \
  -status-admin up

# Verify the LIF is up
network interface show -vserver s3svm
```

> Replace `ontap-sim-01` with the actual node name from step 3.1.
> Replace `e0c` with an available port (check: `network port show -node ontap-sim-01`).
> Replace `192.168.233.20` with an available IP in your hostonly subnet.

### 3.4 — Enable S3 server on the SVM

Using HTTP (port 80) avoids all certificate complications for the demo.

```ontap
vserver object-store-server create \
  -vserver s3svm \
  -object-store-server s3.ontap.local \
  -is-http-enabled true \
  -is-https-enabled false \
  -port 80

# Verify
vserver object-store-server show
```

### 3.5 — Create the S3 user for lakeFS

```ontap
vserver object-store-server user create \
  -vserver s3svm \
  -user lakefs
```

**CRITICAL: The output shows the access key and secret key exactly once.**
Copy them immediately:

```
Access Key: <shown here>
Secret Key: <shown here>
```

Paste these into your `.env` file:
```
ONTAP_S3_ACCESS_KEY=<the access key>
ONTAP_S3_SECRET_KEY=<the secret key>
```

And into `lakefs.yaml` under `blockstore.s3.credentials`.

If you lose the keys, recreate the user:
```ontap
vserver object-store-server user delete -vserver s3svm -user lakefs
vserver object-store-server user create -vserver s3svm -user lakefs
```

### 3.6 — Create the S3 bucket

```ontap
vserver object-store-server bucket create \
  -vserver s3svm \
  -bucket lakefs-data \
  -size 50GB

# Verify
vserver object-store-server bucket show
```

### 3.7 — Grant the lakefs user full access to the bucket

```ontap
vserver object-store-server bucket policy statement create \
  -vserver s3svm \
  -bucket lakefs-data \
  -effect allow \
  -action * \
  -principal lakefs \
  -resource lakefs-data,lakefs-data/*

# Verify policy
vserver object-store-server bucket policy statement show -vserver s3svm -bucket lakefs-data
```

### 3.8 — Verify everything from the ONTAP side

```ontap
# S3 server running?
vserver object-store-server show

# LIF is up?
network interface show -vserver s3svm

# Bucket exists?
vserver object-store-server bucket show

# User and keys?
vserver object-store-server user show
```

---

## Part 4 — Verify from Mac host

From your Mac terminal (NOT inside ONTAP):

```bash
# Load .env values
source .env

# Basic reachability
ping -c 3 192.168.233.20    # replace with your data LIF IP

# HTTP check — should return 200, 301, or 403
curl -v "http://192.168.233.20/lakefs-data/"

# Full S3 credential test (requires awscli: brew install awscli)
AWS_ACCESS_KEY_ID=$ONTAP_S3_ACCESS_KEY \
AWS_SECRET_ACCESS_KEY=$ONTAP_S3_SECRET_KEY \
aws s3 ls s3://lakefs-data/ \
  --endpoint-url "http://192.168.233.20" \
  --region us-east-1
```

If `aws s3 ls` returns empty output (no error) the bucket is accessible.

---

## Quick-reference: IP addresses used in this demo

### If using UTM (Shared Network)

| Role                      | IP               | Notes                          |
|---------------------------|------------------|-------------------------------|
| UTM virtual gateway       | 192.168.64.1     | Set by UTM/QEMU                |
| ONTAP node mgmt LIF       | 192.168.64.10    | Set during cluster wizard      |
| ONTAP cluster mgmt LIF    | 192.168.64.11    | SSH target                     |
| ONTAP S3 data LIF         | 192.168.64.20    | lakeFS S3 endpoint             |

### If using VMware Fusion (Host-only vmnet1)

| Role                      | IP               | Notes                          |
|---------------------------|------------------|-------------------------------|
| Mac (vmnet1 gateway)      | 192.168.233.1    | Set by VMware Fusion           |
| ONTAP node mgmt LIF       | 192.168.233.10   | Set during cluster wizard      |
| ONTAP cluster mgmt LIF    | 192.168.233.11   | SSH target                     |
| ONTAP S3 data LIF         | 192.168.233.20   | lakeFS S3 endpoint             |

> All IPs are examples. Substitute your actual subnet throughout.
> After you know your data LIF IP, update `.env` (ONTAP_S3_ENDPOINT) and
> `lakefs.yaml` (blockstore.s3.endpoint) accordingly.

---

## Troubleshooting ONTAP

| Symptom | Fix |
|---|---|
| `ping` fails to VM | Check VM is running, VMware hostonly network is enabled |
| SSH refused | Try VM console; ensure cluster setup completed |
| `data-s3-server` service policy unknown | ONTAP < 9.8; update or use `-role data -data-protocol` syntax |
| LIF fails to come up | Check port is available: `network port show` |
| HTTP 403 from S3 | Bucket policy missing; re-run step 3.7 |
| Access key lost | Delete user and recreate (step 3.5) |
| Simulator very slow | Normal on x86 emulation; allow 5-10 min on first boot |
