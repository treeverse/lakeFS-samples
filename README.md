# lakefs-samples

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

Once the stack's up and running, open the Jupyter Notebook (http://localhost:8888) and check out the [catalog of sample notebooks](./notebooks/00_index.ipynb) to explore lakeFS. 


### **Don't have a lakeFS Server or Object Store?**

If you want to provision a lakeFS server as well as MinIO for your object store, plus Jupyter then bring up the full stack:

```bash
# make sure we've got the lakeFS hooks content too
git submodule init
git submodule update

docker compose up
```

As above, open the Jupyter Notebook (http://localhost:8888) peruse the [catalog of sample notebooks](./notebooks/00_index.ipynb) to explore lakeFS. 


## Environment Details

* **Jupyter Notebook** is based on the [Jupyter PySpark notebook](https://hub.docker.com/r/jupyter/pyspark-notebook/) and provides an interactive environment in which to explore lakeFS using Python and PySpark. 
* **lakeFS** can be provisioned as part of this environment, or provided by [lakeFS cloud](http://https://lakefs.cloud/) or your [own installation](https://docs.lakefs.io/deploy/).
* If you run lakeFS as part of this environment, **MinIO** is provided as an S3-compatible object store. If you run lakeFS yourself you can use other S3-compatible object stores include S3, GCS, as well as MinIO
* A sample **lakeFS webhooks server** is provided, configured based on using the provided lakeFS server. 

### Containers

![](images/containers.png)

### URLs and login details

* Jupyter http://localhost:8888/

If you've brought up the full stack you'll also have: 

* LakeFS http://localhost:8000/ (`AKIAIOSFODNN7EXAMPLE` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`)
* MinIO http://localhost:9001/ (`minioadmin`/`minioadmin`)
* Spark UI http://localhost:4040/

## Got Questions or Want to Chat?

👉🏻 Join the lakeFS Slack group - https://lakefs.io/slack
