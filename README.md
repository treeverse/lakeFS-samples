# lakefs-samples

[![Check notebooks](https://github.com/treeverse/lakeFS-samples/actions/workflows/check-notebooks.yml/badge.svg?branch=main)](https://github.com/treeverse/lakeFS-samples/actions/workflows/check-notebooks.yml)

_Incorporating the Docker Compose formally known as **Everything Bagel**._

![lakeFS logo](images/logo.png)

**This sample repository captures a collection of notebooks, dockerized applications and code snippets that demonstrate how to use lakeFS.**

_lakeFS is a popular open-source solution for managing data. It provides a consistent and scalable data management layer on top of cloud storage, such as Amazon S3, Azure Blob Storage, or Google Cloud Storage. It allows users to create and manage data in a version-controlled and immutable manner, and offers features such as data governance, data lineage, and data access controls. lakeFS is compatible with a wide range of data processing frameworks and tools._

## Let's Get Started 👩🏻‍💻

Clone this repository

```bash
git clone https://github.com/treeverse/lakeFS-samples.git
cd lakeFS-samples
```

You now have two options: 

### **Run a Notebook server with your existing lakeFS Server**

If you have already [installed lakeFS](https://docs.lakefs.io/deploy/) or are utilizing [lakeFS cloud](https://lakefs.cloud/), all you need to run is the Jupyter notebook server:

```bash
docker compose up jupyter-notebook
```

Once the stack's up and running, open the Jupyter Notebook (http://localhost:8888) and check out the [catalog of sample notebooks](./00_notebooks/00_index.ipynb) to explore lakeFS. 

Once you've finished, run the following to remove all the containers: 

```bash
docker compose down
```

### **Don't have a lakeFS Server or Object Store?**

If you want to provision a lakeFS server as well as MinIO for your object store, plus Jupyter then bring up the full stack:

```bash
docker compose up
```

As above, open the Jupyter Notebook (http://localhost:8888) peruse the [catalog of sample notebooks](./00_notebooks/00_index.ipynb) to explore lakeFS. 


## Environment Details

* **Jupyter Notebook** is based on the [Jupyter PySpark notebook](https://hub.docker.com/r/jupyter/pyspark-notebook/) and provides an interactive environment in which to explore lakeFS using Python and PySpark. 
* **lakeFS** can be provisioned as part of this environment, or provided by [lakeFS cloud](http://https://lakefs.cloud/) or your [own installation](https://docs.lakefs.io/deploy/).
* If you run lakeFS as part of this environment, **MinIO** is provided as an S3-compatible object store. If you run lakeFS yourself you can use other S3-compatible object stores include S3, GCS, as well as MinIO

### Containers

![](images/containers.png)

### URLs and login details

* Jupyter http://localhost:8888/

If you've brought up the full stack you'll also have: 

* LakeFS http://localhost:8000/ (`AKIAIOSFOLKFSSAMPLES` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`)
* MinIO http://localhost:9001/ (`minioadmin`/`minioadmin`)
* Spark UI http://localhost:4040/

## Other Examples

Under the [standalone_examples](./01_standalone_examples/) folder are a set of examples that need to be run on their own. Some use the repository's Docker Compose file and extend it, and others are self-contained and use their own Dockerfile. 

* [Airflow (1)](./01_standalone_examples/airflow-01/) - Four examples of using lakeFS with Airflow: 
    * Versioning DAGs and running pipeline from hooks using a configurable version of DAGs 
    * Isolating Airflow job run and atomic promotion to production
    * Integration of lakeFS with Airflow via Hooks
    * Troubleshooting production issues
* [Airflow (2)](./01_standalone_examples/airflow-02/) - lakeFS + Airflow
* [Azure Databricks](./01_standalone_examples/azure-databricks/)
* [AWS Databricks](./01_standalone_examples/aws-databricks/)
* [lakeFS + Dagster](./01_standalone_examples/dagster-integration/)
* [Labelbox integration](./01_standalone_examples/labelbox-integration/)
* [How to migrate or clone a repo](./01_standalone_examples/migrate-or-clone-repo/)
## Got Questions or Want to Chat?

👉🏻 Join the lakeFS Slack group - https://lakefs.io/slack
