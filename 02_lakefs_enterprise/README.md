# lakeFS Enterprise

![lakeFS logo](../images/logo.png)

**This sample repository captures a collection of notebooks, dockerized applications and code snippets that demonstrate how to use [lakeFS Enterprise](https://docs.lakefs.io/understand/enterprise/).**

## Let's Get Started 👩🏻‍💻

[Contact Sales](https://lakefs.io/contact-sales/) to get the license file for lakeFS Enterprise.

Clone this repository

```bash
git clone https://github.com/treeverse/lakeFS-samples.git
cd lakeFS-samples/02_lakefs_enterprise
```

##### **Multiple Storage Backends**

If you want to use lakeFS [Multiple Storage Backends](https://docs.lakefs.io/latest/howto/multiple-storage-backends/) feature then change "lakeFS-samples/02_lakefs_enterprise/docker-compose.yml" file to update credentials for AWS S3 and/or Azure Blob Storage. If you want to use Google Cloud Storage (GCS) then copy GCP Service Account key JSON file to "lakeFS-samples/02_lakefs_enterprise" folder and change the file name in Docker Compose file. Refer to [Multiple Storage Backends documentation](https://docs.lakefs.io/latest/howto/multiple-storage-backends/) for additional information.

If you DO NOT want to use lakeFS Multiple Storage Backends feature then don't change the Docker Compose file.

##### **AWS Glue Catalog Sync of Iceberg Tables**

If you want to sync Iceberg tables created in lakeFS to AWS Glue Catalog then export the following environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
```

##### **Copy the lakeFS license file and run a lakeFS Enterprise server**

Copy the lakeFS license file to "lakeFS-samples/02_lakefs_enterprise" folder, then change lakeFS license file name and installation ID in the following command and run the command to provision a lakeFS Enterprise server as well as MinIO for your object store, plus Jupyter:

```bash
LAKEFS_LICENSE_FILE_NAME=license-org-name-installation-id.token LAKEFS_INSTALLATION_ID=installation-id docker compose up
```

Once the stack's up and running, open the Jupyter Notebook (http://localhost:8894) and check out the [catalog of sample notebooks](../00_notebooks/00_index.ipynb) to explore lakeFS. 

Once you've finished, run the following to remove all the containers: 

```bash
docker compose down
```

##### **Persisted data**

Data for Postgres, MinIO and local storage is persisted in the **lakefs-enterprise-samples-data** folder, so your repositories, users and policies are still there after `docker compose down` (or `docker compose down -v`) and `docker compose up`.

Because of this, sample notebooks that create users, groups, policies or repositories fail with `409` (already exists) errors if you run them a second time. To start from a clean environment, remove the containers and delete the folder:

```bash
docker compose down
rm -rf lakefs-enterprise-samples-data
```

On Linux, Docker creates this folder as root and the Postgres data is owned by the container's `postgres` user, so use `sudo rm -rf lakefs-enterprise-samples-data` instead.

## Environment Details

* **Jupyter Notebook** is based on the [Jupyter PySpark notebook](https://hub.docker.com/r/jupyter/pyspark-notebook/) and provides an interactive environment in which to explore lakeFS using Python and PySpark. 
* **lakeFS Enterprise** is provisioned as part of this environment.
* **MinIO** is provided as an S3-compatible object store. You can use other S3-compatible object stores include S3, GCS, as well as Azure Blob Storage.
* **Postgres, MinIO and local storage data** is persisted in the **lakefs-enterprise-samples-data** folder. See [Persisted data](#persisted-data) to start from a clean environment.

### URLs and login details

* Jupyter http://localhost:8894/
* lakeFS http://localhost:8084/ (`AKIAIOSFOLKFSSAMPLES` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`)
* MinIO http://localhost:9005/ (`minioadmin`/`minioadmin`)
* Spark UI http://localhost:4044/

## Got Questions or Want to Chat?

👉🏻 Join the lakeFS Slack group - https://lakefs.io/slack
