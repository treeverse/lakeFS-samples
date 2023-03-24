# Integration of lakeFS with Spark, Python, Delta Lake, Airflow and Hooks

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/) project.

This repository includes a Jupyter Notebook which you can run on your local machine demonstrating data lineage with lakeFS. 
In this example, data sets (employees & salaries) are ingested through two separated branches. Then, merged together on a transformation 
branch. And finally, promoted to the production branch.

At the very end of the process, the lakeFS "Blame" functionality (``` log_commits ```) is used to trace the origin of a specific
file or dataset.

## Prerequisites
* Docker installed on your local machine
* lakeFS installed and running on your local machine or on a server or in the cloud. If you don't have lakeFS already running then spin up a [lakeFS Cloud](https://lakefs.cloud/) which provides lakeFS server on-demand with a single click or refer to [lakeFS Quickstart](https://docs.lakefs.io/quickstart/) doc.

## Setup

1. Start by cloning this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/08-data-lineage
   ```

2. Run following commands to download and run Docker container which includes Python, Spark, Jupyter Notebook, JDK, Hadoop binaries, lakeFS Python client and Airflow (Docker image size is around 4.5GB):

   ```bash
      docker build -t lakefs-lineage-demo .

      docker run -d -p 8888:8888 -p 4040:4040 -p 8080:8080 --user root -e GRANT_SUDO=yes -v $PWD:/home/jovyan -v $PWD/jupyter_notebook_config.py:/home/jovyan/.jupyter/jupyter_notebook_config.py --name lakefs-lineage-demo lakefs-lineage-demo
   ```

3. Open JupyterLab UI [http://127.0.0.1:8888/](http://127.0.0.1:8888/) in your web browser.

## Demo Instructions

Once you have successfully completed setup then open any notebook from JupyterLab UI and follow the instructions.

