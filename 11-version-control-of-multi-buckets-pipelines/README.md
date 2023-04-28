# Version Control of multi-buckets pipelines

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/oreilly-course) project.

This repository includes a Jupyter Notebook which you can run on your local machine.

## Introduction
In data engineering pipelines, it is common to have distinct buckets that serve different purposes. These buckets are typically named and categorized based on their respective stages in the data processing pipeline.

When implementing lakeFS, it may be necessary to maintain separate physical buckets for each stage. However, it is important to version control all changes made to each bucket and link between different versions to track the evolution of the data through the pipeline.

## Prerequisites
* Docker installed on your local machine
* This demo requires connecting to a lakeFS Server. You can either install lakeFS Server locally (https://docs.lakefs.io/quickstart.html), or spin up for free on the lakeFS cloud (https://lakefs.cloud).

## Setup

1. Start by cloning this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/11-version-control-of-multi-buckets-pipelines
   ```

2. Run following commands to download and run Docker container which includes Python, Spark, Jupyter Notebook, JDK, Hadoop binaries and lakeFS Python client (Docker image size is around 4GB):

   ```bash
      docker build -t lakefs-multi-buckets-pipelines-demo .

      docker run -d -p 8888:8888 -p 4040:4040 --user root -e GRANT_SUDO=yes -v $PWD:/home/jovyan -v $PWD/jupyter_notebook_config.py:/home/jovyan/.jupyter/jupyter_notebook_config.py --name lakefs-multi-buckets-pipelines-demo lakefs-multi-buckets-pipelines-demo
   ```

3. Open JupyterLab UI [http://127.0.0.1:8888/](http://127.0.0.1:8888/) in your web browser.

## Demo Instructions

Once you have successfully completed setup then open "Multi-buckets Pipelines Demo" notebook from JupyterLab UI and follow the instructions.