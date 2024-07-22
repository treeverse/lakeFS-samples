# Integration of lakeFS with Airflow and Hooks

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/oreilly-course) project.

This repository includes following Jupyter Notebooks which you can run on your local machine:

1. Airflow Demo Existing DAG:
* Integration of lakeFS with Airflow
* Use Case: Isolating Airflow job run and atomic promotion to production

2. Airflow Demo New DAG:
* Integration of lakeFS with Airflow
* Use Case: Troubleshooting production issues

3. Hooks Airflow Demo:
* Integration of lakeFS with Airflow via Hooks
* Use Case: Isolated Ingestion & ETL Environment

4. Airflow DAG Versioning Demo:
* Integration of lakeFS with Airflow via Hooks
* Use Case: Versioning DAGs and running pipeline from hooks using a configurable version of DAGs

5. Databricks:
* Integration of lakeFS with Airflow and Databricks
* Use Case: Run Databricks notebook via Airflow DAG

6. Iceberg:
* Integration of lakeFS with Airflow and Iceberg
* Use Case: Isolating Airflow job run and atomic promotion to production

## Prerequisites
* Docker installed on your local machine
* lakeFS installed and running on your local machine or on a server or in the cloud. If you don't have lakeFS already running then either use [lakeFS Playground](https://demo.lakefs.io/) which provides lakeFS server on-demand with a single click or refer to [lakeFS Quickstart](https://docs.lakefs.io/quickstart/) doc.

## Setup

1. Start by cloning this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/01_standalone_examples/airflow-01
   ```

2. Run following commands to download and run Docker container which includes Python, Spark, Jupyter Notebook, JDK, Hadoop binaries, lakeFS Python client and Airflow (Docker image size is around 4.5GB):

   ```bash
      docker build -t lakefs-airflow-integration-demo .

      docker run -d -p 18888:8888 -p 14040:4040 -p 8080:8080 --user root -e GRANT_SUDO=yes -v $PWD:/home/jovyan -v $PWD/jupyter_notebook_config.py:/home/jovyan/.jupyter/jupyter_notebook_config.py --name lakefs-airflow-integration-demo lakefs-airflow-integration-demo
   ```

3. Open JupyterLab UI [http://127.0.0.1:18888/](http://127.0.0.1:18888/) in your web browser.

## Demo Instructions

Once you have successfully completed setup then open any notebook from JupyterLab UI and follow the instructions.

