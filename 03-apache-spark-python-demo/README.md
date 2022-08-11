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

2. Run one of the following commands to download and run Spark Docker container which includes Python, Jupyter Notebook, pyspark, JDK and Hadoop binaries (Docker image size is around 3.8GB):

	2.1. Run this command if using amd64 (x86_64) CPU Architectures platforms e.g macOS with Intel chip:
   ```bash
   docker run -d -p 8888:8888 -p 4040:4040 --user root -e GRANT_SUDO=yes -v $PWD:/home/jovyan --name lakefs-spark-python-demo jupyter/pyspark-notebook
   ```

	2.2. Run this command if using aarch64 CPU Architectures platforms e.g. macOS with Apple chip:
   ```bash
   docker run -d -p 8888:8888 -p 4040:4040 --user root -e GRANT_SUDO=yes -v $PWD:/home/jovyan --name lakefs-spark-python-demo jupyter/pyspark-notebook:aarch64-python-3.10.5
   ```

3. Launch a Bash terminal within a Docker container:

   ```bash
   docker exec -it lakefs-spark-python-demo /bin/bash
   ```

4. Display the currently running server along with the tokens:

   ```bash
   jupyter server list
   ```

5. Open [http://127.0.0.1:8888/](http://127.0.0.1:8888/) in your web browser.

6. Use the token returned by “jupyter server list” command to login to JupyterLab UI and open “Installation” notebook and run it.

## Demo Instructions

Once you have successfully completed setup then open "Spark Demo" or "Delta Lake Demo" notebook from JupyterLab UI and follow the instructions.

