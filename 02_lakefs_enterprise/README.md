# lakeFS Enterprise

![lakeFS logo](../images/logo.png)

**This sample repository captures a collection of notebooks, dockerized applications and code snippets that demonstrate how to use [lakeFS Enterprise](https://docs.lakefs.io/understand/enterprise/).**

## Let's Get Started 👩🏻‍💻

Clone this repository

```bash
git clone https://github.com/treeverse/lakeFS-samples.git
cd lakeFS-samples/02_lakefs_enterprise
```

### **Run a lakeFS Enterprise server**

Login to [Treeverse Dockerhub](https://hub.docker.com/u/treeverse) by using the granted token so lakeFS Enterprise proprietary image can be retrieved. [Contact Sales](https://lakefs.io/contact-sales/) to get the token and license file for lakeFS Enterprise:

```bash
docker login -u externallakefs
```

If you want to use lakeFS [Multiple Storage Backends](https://docs.lakefs.io/latest/howto/multiple-storage-backends/) feature then change "lakeFS-samples/02_lakefs_enterprise/docker-compose.yml" file to update credentials for AWS S3 and/or Azure Blob Storage. If you want to use Google Cloud Storage (GCS) then copy GCP Service Account key JSON file to "lakeFS-samples/02_lakefs_enterprise" folder and change the file name in Docker Compose file. Refer to [Multiple Storage Backends documentation](https://docs.lakefs.io/latest/howto/multiple-storage-backends/) for additional information.

If you DO NOT want to use lakeFS Multiple Storage Backends feature then don't change the Docker Compose file.

Copy the lakeFS license file to "lakeFS-samples/02_lakefs_enterprise" folder, then change lakeFS license file name and installation ID in the following command and run the command to provision a lakeFS Enterprise server as well as MinIO for your object store, plus Jupyter:

```bash
LAKEFS_LICENSE_FILE_NAME=license-org-name-installation-id.token LAKEFS_INSTALLATION_ID=installation-id docker compose up
```

Once the stack's up and running, open the Jupyter Notebook (http://localhost:8894) and check out the [catalog of sample notebooks](../00_notebooks/00_index.ipynb) to explore lakeFS. 

Once you've finished, run the following to remove all the containers: 

```bash
docker compose down
```

## Environment Details

* **Jupyter Notebook** is based on the [Jupyter PySpark notebook](https://hub.docker.com/r/jupyter/pyspark-notebook/) and provides an interactive environment in which to explore lakeFS using Python and PySpark. 
* **lakeFS Enterprise** is provisioned as part of this environment.
* **MinIO** is provided as an S3-compatible object store. You can use other S3-compatible object stores include S3, GCS, as well as Azure Blob Storage.

### URLs and login details

* Jupyter http://localhost:8894/
* lakeFS http://localhost:8084/ (`AKIAIOSFOLKFSSAMPLES` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`)
* MinIO http://localhost:9005/ (`minioadmin`/`minioadmin`)
* Spark UI http://localhost:4044/

## Got Questions or Want to Chat?

👉🏻 Join the lakeFS Slack group - https://lakefs.io/slack
