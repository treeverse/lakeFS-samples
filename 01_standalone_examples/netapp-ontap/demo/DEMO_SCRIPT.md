# lakeFS + NetApp ONTAP — Phase 1 Live Demo Script

**Use case:** ML Feature Engineering for Customer Churn Prediction
**Duration:** ~10 minutes
**Tabs to have open before you start:**
1. AWS Console → FSx → `lakefs-ontap-demo`
2. lakeFS UI → `http://<EC2_PUBLIC_IP>:8000`
3. Terminal (SSH into EC2, lakeFS running)

---

## Opening (30 seconds)

> "Today I'm going to show you how lakeFS brings Git-like data versioning
> to NetApp ONTAP storage — so your ML teams can branch, commit, diff,
> and merge datasets the same way engineers do with code.
> No data copies, no manual versioning, no 'which CSV is the right one' chaos."

---

## Tab 1 — AWS FSx Console (2 min)

**Show the filesystem summary page.**

> "This is real NetApp ONTAP — running as a managed service on AWS.
> Same ONTAP you'd run on-prem, same CLI, same storage efficiency features.
> One terabyte of SSD, 384 MB/s throughput, storage efficiency enabled —
> deduplication, compression, compaction all on by default."

**Click Storage Virtual Machines → fsx → Endpoints tab.**

> "ONTAP exposes multiple protocols from a single SVM —
> NFS, iSCSI, and crucially for us: native S3.
> No translation layer, no gateway — ONTAP speaks S3 natively on port 80.
> This is what lakeFS connects to as its backing store."

**Click Volumes tab.**

> "You can see the vol1 FlexVol here. This is where the actual data lives.
> Everything lakeFS writes goes through the ONTAP S3 endpoint,
> gets stored here, and benefits from all of ONTAP's enterprise capabilities."

---

## Tab 2 — lakeFS UI (5 min)

**Navigate to `http://<EC2_PUBLIC_IP>:8000` → churn-features repository.**

> "Now let's look at lakeFS. This is our repository — think of it like
> a GitHub repo, but instead of code files, it contains ML training data.
> The physical storage is that ONTAP S3 bucket we just looked at."

**Click on the `main` branch → data/ → customers.csv**

> "This is our baseline dataset — 20 customer records with features like
> tenure, monthly charges, contract type. This is what the model was
> originally trained on. It's committed, versioned, immutable."

**Switch to the `feature-add-payment-history` branch.**

> "Now here's where it gets interesting. A data scientist wanted to
> engineer a new feature — payment_history_score — derived from billing data.
> Instead of overwriting the original dataset, they created a branch.
> Zero data copied. lakeFS uses copy-on-write — the branch costs nothing
> until something actually changes."

**Click on data/ → customers.csv on the branch.**

> "On this branch, the dataset has the new payment_history_score column.
> The original main branch is completely untouched."

**Click Commits tab on the branch.**

> "Full commit history. Every change has a hash, a timestamp, a committer,
> and rich metadata. This one has experiment_id=exp-2025-001.
> You can always answer: what data was used to train this model, and when?"

**Click Compare → select main as the base.**

> "This is the diff. Exactly one file changed: data/customers.csv.
> One column added. This is how you do code review for data —
> before you merge a new feature into your production dataset,
> you can see exactly what changed."

**Click on the main branch → Commits tab.**

> "After the team validated the experiment — AUC improved — they merged
> the branch back to main. Just like a pull request. The merge commit
> is right here: 'payment_history_score feature — validated, AUC +0.03'.
> Main now has the engineered feature. And we have a complete audit trail
> of how we got here."

---

## Tab 3 — ONTAP CLI (2 min, optional for technical audiences)

**Switch to the terminal and SSH into EC2, then SSH to ONTAP:**
```
ssh fsxadmin@<FSX_MANAGEMENT_IP>
```

Password: `Netapp1!`

> "For the technically curious — let's look at this from the ONTAP side."

```
vserver object-store-server bucket show -vserver fsx -instance
```

> "There's our lakefs-data bucket — native ONTAP S3, no gateway."

```
vserver object-store-server bucket usage show -vserver fsx
```

> "Bucket usage — every byte lakeFS wrote is accounted for here."

```
volume show -vserver fsx -fields size,used,available
```

> "Three volumes. The fg_oss_* FlexGroup is where ONTAP physically stores
> the S3 objects — that's where all the lakeFS data lives on disk.
> Notice vol1 is nearly empty — the S3 data goes into the object store volume."

```
volume efficiency show -vserver fsx
```

> "Storage efficiency running automatically — dedup and compression
> on everything lakeFS writes, with no configuration required."

**Then exit ONTAP and show the raw S3 objects from EC2:**
```
exit
```

```bash
AWS_ACCESS_KEY_ID=$ONTAP_S3_ACCESS_KEY \
AWS_SECRET_ACCESS_KEY="$ONTAP_S3_SECRET_KEY" \
AWS_DEFAULT_REGION=us-east-1 \
aws s3 ls s3://lakefs-data/ \
  --recursive \
  --endpoint-url http://<ONTAP_S3_ENDPOINT_IP>
```

> "Three levels of visibility in one demo:
> The logical dataset in lakeFS, the S3 objects on ONTAP,
> and the physical blocks in the FlexGroup volume.
> Full transparency from dataset to disk."

---

## Closing (30 seconds)

> "So what did we just see?
>
> NetApp ONTAP provides enterprise-grade S3 storage — on-prem or in the cloud.
> lakeFS sits on top and adds the version control layer that data teams need:
> branches for safe experimentation, commits for reproducibility,
> diffs for data review, merges for controlled promotion to production.
>
> No infrastructure changes. No data copies. No workflow disruption.
> Your data engineers work in Python, Spark, or SQL — they don't need to
> know anything about lakeFS internals. It just looks like S3 to them.
>
> And your storage team keeps running ONTAP — with all the efficiency,
> protection, and compliance features they already rely on."

---

## Q&A Prep

**Q: Does lakeFS work with on-prem ONTAP, not just FSx?**
> Yes — any ONTAP with S3 enabled. The configuration is identical.
> On-prem, FSx, or Cloud Volumes ONTAP.

**Q: What happens to storage when you create branches?**
> Copy-on-write — a branch costs zero storage until data changes.
> lakeFS uses content addressing, so identical blocks are never stored twice.

**Q: Can we plug this into our existing ML pipelines?**
> Yes. lakeFS exposes an S3-compatible endpoint. Any tool that reads S3
> works with lakeFS — Spark, pandas, SageMaker, Databricks, all of it.

**Q: What about access control?**
> lakeFS has its own RBAC on top of ONTAP's native access controls.
> You can lock down who can merge to main, who can create branches, etc.

**Q: How is this different from just using S3 versioning?**
> S3 versioning versions individual objects. lakeFS versions entire datasets
> atomically — a commit captures the state of thousands of files at once.
> You get cross-file consistency, branching, merging, and diffs.
> S3 versioning can't give you that.
