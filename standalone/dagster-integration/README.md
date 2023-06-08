# Integration of lakeFS with Dagster

## Prerequisites

* Docker installed on your local machine

## Setup

1. Start by cloning this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/standalone/dagster-integration
   ```

2. Run the following to provision the stack which includes Python, Spark, Jupyter Notebook, JDK, Hadoop binaries and lakeFS Python client


   ```bash
      docker compose up 
   ```

3. Open JupyterLab UI [http://127.0.0.1:8888/](http://127.0.0.1:8888/) in your web browser.

## Demo Instructions

Once you have successfully completed setup then open one of the provided notebooks from JupyterLab UI: 

* Dagster Demo Existing DAG
* Dagster Demo New DAG