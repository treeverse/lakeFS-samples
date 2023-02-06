# Integration of lakeFS with Spark, Python, Delta Lake, Airflow and Hooks

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/oreilly-course) project.

This repository includes following Jupyter Notebooks which you can run on your local machine:

1. Spark Demo:
1.1. Integration of lakeFS with Apache Spark and Python
1.2. Use Case: Isolated Testing Environment

2. Delta Lake Demo:
2.1. Integration of lakeFS with Delta Lake
2.2. Use Cases: Isolating ETL job and atomic promotion to production. Atomic rollback of Multi-Table Transactions.

3. Airflow Demo Existing DAG:
3.1. Integration of lakeFS with Airflow
3.2. Use Case: Isolating Airflow job run and atomic promotion to production

4. Airflow Demo New DAG:
4.1. Integration of lakeFS with Airflow
4.2. Use Case: Troubleshooting production issues

5. Hooks Airflow Demo:
5.1. Integration of lakeFS with Airflow via Hooks
5.2. Use Case: Isolated Ingestion & ETL Environment

6. Hooks Schema Validation:
6.1. Using Hooks or Git like actions
6.2. Use Cases: Don't allow PII data and schema changes

7. Reprocess Data Demo:
7.1. Integration of lakeFS with Apache Spark and Python
7.2. Use Case: Reprocess and Backfill Data with new ETL logic

## Prerequisites
* Docker installed on your local machine
* lakeFS installed and running on your local machine or on a server or in the cloud. If you don't have lakeFS already running then either use [lakeFS Playground](https://demo.lakefs.io/) which provides lakeFS server on-demand with a single click or refer to [lakeFS Quickstart](https://docs.lakefs.io/quickstart/) doc.

## Setup

1. Start by cloning this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/03-apache-spark-python-demo
   ```

2. Run following commands to download and run Docker container which includes Python, Spark, Jupyter Notebook, JDK, Hadoop binaries, lakeFS Python client and Airflow (Docker image size is around 4.5GB):

   ```bash
      docker build -t lakefs-spark-python-demo .

      docker run -d -p 8888:8888 -p 4040:4040 -p 8080:8080 --user root -e GRANT_SUDO=yes -v $PWD:/home/jovyan -v $PWD/jupyter_notebook_config.py:/home/jovyan/.jupyter/jupyter_notebook_config.py --name lakefs-spark-python-demo lakefs-spark-python-demo
   ```

3. Open JupyterLab UI [http://127.0.0.1:8888/](http://127.0.0.1:8888/) in your web browser.

## Demo Instructions

Once you have successfully completed setup then open any notebook from JupyterLab UI and follow the instructions.

