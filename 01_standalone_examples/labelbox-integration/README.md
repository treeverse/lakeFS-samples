# Labelbox Integration

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/oreilly-course) project.

This repository includes a Jupyter Notebook which you can run on your local machine.

## Prerequisites
* Docker installed on your local machine
* This demo requires connecting to a lakeFS Server. You can either install lakeFS Server locally (https://docs.lakefs.io/quickstart.html), or spin up for free on the lakeFS cloud (https://lakefs.cloud). 
* This demo also requires connecting to Labelbox. You can signup for free for Labelbox (https://app.labelbox.com/signup)

## Setup

1. Start by cloning this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/01_standalone_examples/labelbox-integration
   ```

2. Run following commands to download and run Docker container which includes Python, Spark, Jupyter Notebook, JDK, Hadoop binaries and lakeFS Python client (Docker image size is around 4GB):

   ```bash
      docker build -t lakefs-labelbox-integration-demo .

      docker run -d -p 8888:8888 -p 4040:4040 --user root -e GRANT_SUDO=yes -v $PWD:/home/jovyan -v $PWD/jupyter_notebook_config.py:/home/jovyan/.jupyter/jupyter_notebook_config.py --name lakefs-labelbox-integration-demo lakefs-labelbox-integration-demo
   ```

3. Open JupyterLab UI [http://127.0.0.1:8888/](http://127.0.0.1:8888/) in your web browser.

## Demo Instructions

Once you have successfully completed setup then open "Labelbox Demo" notebook from JupyterLab UI and follow the instructions.
