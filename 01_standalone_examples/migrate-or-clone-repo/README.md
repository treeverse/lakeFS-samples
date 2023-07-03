# Migrate or clone a lakeFS repository

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/oreilly-course) project.

This repository includes a Jupyter Notebook which you can run on your local machine.

## Prerequisites
* Docker installed on your local machine
* Source and a target lakeFS environments (you can deploy one independently (https://docs.lakefs.io/deploy/) or use the hosted solution lakeFS Cloud (https://lakefs.cloud)
* Source repository
* Object storage for target repository but don't create target repository ahead of time (you will create the target repo in the notebook)

## Setup

1. Start by cloning this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/01_standalone_examples/migrate-or-clone-repo
   ```

2. Run following commands to download and run Docker container which includes Python, Spark, Jupyter Notebook, JDK, Hadoop binaries and lakeFS Python client (Docker image size is around 4GB):

   ```bash
      docker build -t lakefs-migrate-or-clone-repo .

      docker run -d -p 48888:8888 -p 44040:4040 --user root -e GRANT_SUDO=yes -v $PWD:/home/jovyan -v $PWD/jupyter_notebook_config.py:/home/jovyan/.jupyter/jupyter_notebook_config.py --name lakefs-migrate-or-clone-repo lakefs-migrate-or-clone-repo
   ```

3. Open JupyterLab UI [http://127.0.0.1:48888/](http://127.0.0.1:48888/) in your web browser.

## Demo Instructions

Once you have successfully completed setup then open either "Migrate or Clone AWS Repo", "Migrate or Clone Azure Repo", "Migrate or Clone Local Repo to AWS" or "Migrate or Clone Local Repo to Azure" notebook (depending upon your requirement) from JupyterLab UI and follow the instructions.
