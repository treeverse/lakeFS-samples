# Integration of lakeFS with Spark and Python

## Prerequisites

* Docker installed on your local machine

## Setup

1. Start by cloning this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/01_standalone_examples/spark
   ```

2. Run the following to provision the stack which includes Python, Spark, Jupyter Notebook, JDK, Hadoop binaries and lakeFS Python client


   ```bash
   docker compose up 
   ```

   Alternatively, if you want to provision a lakeFS server as well as MinIO for your object store, plus Jupyter then bring up the full stack:

   ```bash
   docker compose --profile local-lakefs up
   ```

3. Open JupyterLab UI [http://127.0.0.1:8888/](http://127.0.0.1:8888/) in your web browser.

## Demo Instructions

Once you have successfully completed setup then open spark-demo notebook from JupyterLab UI.