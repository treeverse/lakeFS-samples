# Integration of lakeFS with Spark and Python

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/oreilly-course) project.

This repository includes Jupyter Notebooks and a sample CSV file to demonstrate integration of lakeFS with Apache Spark and Python. You can run this demo on your local machine.

## Prerequisites
* Docker installed on your local machine
* lakeFS installed and running on your local machine or on a server or in the cloud

## Setup

1. Start by cloning this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/03-apache-spark-python-demo
   ```

2. Run following commands to download and run Docker container which includes Python, Spark, Jupyter Notebook, JDK, Hadoop binaries and lakeFS Python client (Docker image size is around 4GB):

   ```bash
      docker build -t lakefs-spark-python-demo .

      docker run -d -p 8888:8888 -p 4040:4040 -p 8080:8080 --user root -e GRANT_SUDO=yes -v $PWD:/home/jovyan -v $PWD/jupyter_notebook_config.py:/home/jovyan/.jupyter/jupyter_notebook_config.py --name lakefs-spark-python-demo lakefs-spark-python-demo
   ```

3. Open [http://127.0.0.1:8888/](http://127.0.0.1:8888/) in your web browser.

## Demo Instructions

Once you have successfully completed setup then open "Spark Demo" or "Delta Lake Demo" notebook from JupyterLab UI and follow the instructions.

