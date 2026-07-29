# Walkthrough — what to look at after running the demo

Once `demo/demo_flow.py` has completed, the same dataset is visible at three
layers: as a versioned repository in lakeFS, as content-addressed objects in the
ONTAP S3 bucket, and as physical blocks in an ONTAP volume. This walks through
all three.

Prerequisites: the demo script has run successfully, and you have the lakeFS UI
open at `http://<EC2_PUBLIC_IP>:8000`.

---

## 1. The versioned dataset in lakeFS

Open the **`churn-features`** repository.

**`main` branch → `data/` → `customers.csv`**

The baseline dataset: 20 customer records with features like tenure, monthly
charges, and contract type. Committed, versioned, immutable.

**Switch to the `feature-add-payment-history` branch**

The same path now has an extra column, `payment_history_score`, derived from
billing data. `main` is untouched. Creating the branch copied no data — lakeFS
branches are metadata operations, so they cost nothing until something changes.

**Commits tab, on the branch**

Each commit carries a hash, timestamp, committer, and arbitrary metadata. The
feature commit records `experiment_id=exp-2025-001`, the derivation method, and
the measured AUC improvement — enough to answer "what data trained this model,
and when" from the commit alone.

**Compare tab → base `main`, compared `feature-add-payment-history`**

Exactly one changed file: `data/customers.csv`. This is the reviewable unit — the
data equivalent of a pull request diff, available before anything merges into the
production dataset.

**`main` branch → Commits tab**

After validation the branch was merged. The merge commit is on `main`, and the
full path from baseline to merged feature is in the history.

---

## 2. The S3 objects on ONTAP

lakeFS stores data as content-addressed objects in the ONTAP S3 bucket. From the
EC2 host:

```bash
pip3 install awscli            # installs to ~/.local/bin
export PATH=$PATH:~/.local/bin

AWS_ACCESS_KEY_ID=<ONTAP_ACCESS_KEY> \
AWS_SECRET_ACCESS_KEY="<ONTAP_SECRET_KEY>" \
AWS_DEFAULT_REGION=us-east-1 \
aws s3 ls s3://lakefs-data/ \
  --recursive \
  --endpoint-url http://<SVM_MANAGEMENT_IP>
```

Objects appear under the repository's storage namespace, with lakeFS metadata
under a `_lakefs/` prefix and data objects named by content hash rather than by
path. Identical content is stored once regardless of how many branches or commits
reference it; the human-readable paths you saw in the UI live in the metadata.

---

## 3. The physical storage in ONTAP

SSH from EC2 into the ONTAP CLI:

```bash
ssh fsxadmin@<FSX_MANAGEMENT_IP>
```

```
vserver object-store-server bucket show -vserver fsx -instance
```
The `lakefs-data` bucket, served by ONTAP's native S3 — no gateway or translation
layer in front of it.

```
vserver object-store-server bucket show -vserver fsx -fields bucket,volume,size,logical-used
```
Bucket usage as ONTAP accounts for it, and which volume hosts it:

```
vserver bucket      volume            size  logical-used
------- ----------- ----------------- ----- ------------
fsx     lakefs-data fg_oss_1785275435 500GB 296KB
```

```
volume show -vserver fsx -fields size,used,available
```
The `fg_oss_*` FlexGroup is where ONTAP physically stores the S3 objects. Note
that `vol1` stays nearly empty — object-store data goes to the FlexGroup, not the
NFS volume.

```
volume efficiency show -vserver fsx
```
Deduplication and compression apply to everything lakeFS writes, with no
lakeFS-side configuration.

```
exit
```

---

## Notes

**Where this configuration applies.** Any ONTAP with S3 enabled works the same
way — on-premises, FSx for NetApp ONTAP, or Cloud Volumes ONTAP. Only the
endpoint address changes.

**Branch storage cost.** Branching is a metadata operation. A branch consumes no
additional space until data on it changes, and because objects are
content-addressed, identical content is never stored twice.

**Integration with existing tooling.** lakeFS exposes an S3-compatible endpoint,
so tools that already read S3 — Spark, pandas, SageMaker, Databricks — can read
from a lakeFS branch without modification.

**Relationship to S3 object versioning.** S3 versioning tracks revisions of
individual objects. lakeFS commits capture the state of an entire dataset
atomically, which is what makes cross-file consistency, branching, diffing, and
merging possible.

**Access control.** lakeFS has its own RBAC governing who can commit, merge, or
create branches, layered on top of ONTAP's native access controls on the bucket.
