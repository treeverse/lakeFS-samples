# Integration of lakeFS with Glue Catalog and Trino

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/oreilly-course) project.

This repository includes a Jupyter Notebook which you can run on your local machine. This notebook demonstrates integration of AWS Trino, utilizing Glue Catalog, with lakeFS.

## Prerequisites
* lakeFS installed and running in your AWS environment or in the lakeFS Cloud. If you don't have lakeFS already running then either use [lakeFS Cloud](https://lakefs.cloud/) which provides free lakeFS server on-demand with a single click or [Deploy lakeFS on AWS](https://docs.lakefs.io/howto/deploy/aws.html).

## Setup and Demo Instructions

1. Start by cloning this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/01_standalone_examples/aws-glue-trino
   ```

2. Change `lakeFS Endpoint URL`, `Access Key` and `Secret Key` in `trino_configurations.json` file included in the Git repo in `lakeFS-samples/01_standalone_examples/aws-glue-trino` folder.

3. Run following AWS CLI command to create an EMR cluster. Change AWS `region_name`, `log-uri`, `ec2_subnet_name` before running the command. lakeFS Python SDK requires Python v3.9 or above. Python v3.9 is supported starting with EMR v7.0.0.

   ```bash
   aws emr create-cluster \
       --release-label emr-7.0.0 \
       --applications Name=Trino Name=JupyterEnterpriseGateway Name=Spark \
       --configurations file://trino_configurations.json \
       --region region_name \
       --name lakefs_glue_trino_demo_cluster \
       --log-uri  s3://bucket-name/trino/logs/ \
       --instance-type m5.xlarge \
       --instance-count 1 \
       --service-role EMR_DefaultRole \
       --ec2-attributes  InstanceProfile=EMR_EC2_DefaultRole,SubnetId=ec2_subnet_name
   ```

4. Set up an [AWS EMR Studio](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-studio-set-up.html). Use the same subnet as used to create EMR cluster.

5. Create and launch an [AWS EMR Studio Workspace](https://docs.aws.amazon.com//emr/latest/ManagementGuide/emr-studio-configure-workspace.html#emr-studio-create-workspace).

6. Attach an [AWS EMR cluster to a Workspace](https://docs.aws.amazon.com//emr/latest/ManagementGuide/emr-studio-create-use-clusters.html)

7. Upload `trino-glue-demo-notebook` included in the Git repo in `lakeFS-samples/01_standalone_examples/aws-glue-trino` folder to AWS EMR Studio Workspace. Open this notebook, select PySpark kernel to run the notebook and follow the instructions in the notebook.

8. Run following command to terminate the EMR Cluster once you finish the demo. Change `cluster_id` by the ID returned by `aws emr create-cluster` command or check `cluster_id` in EMR UI.

   ```bash
   aws emr terminate-clusters --cluster-ids cluster_id
   ```

9. Stop or Delete [AWS EMR Studio Workspace](https://docs.aws.amazon.com//emr/latest/ManagementGuide/emr-studio-configure-workspace.html#emr-studio-delete-workspace) once you finish the demo.
