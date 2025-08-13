# Fast Data Loading and Reproducibility for Deep Learning Workloads with lakeFS Mount

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/oreilly-course) project.

This demo includes a Jupyter Notebook which you can run on your local machine.

## Prerequisites
* Docker installed on your local machine
* Watch [this video](https://www.youtube.com/watch?v=BgKuoa8LAaU) to understand the use case as well as the demo.
* [Contact lakeFS](https://lakefs.io/contact-sales/) to get the lakeFS Everest binary for Linux x86_64 OS. Download and save the binary on your laptop.
* OPTIONAL: [Contact lakeFS](https://lakefs.io/contact-sales/) to get the token and license file for lakeFS Enterprise if you want to provision lakeFS Enterprise server.

## Setup

1. Start by cloning this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/01_standalone_examples/lakefs-mount-demo
   ```

2. You now have two options: 

   ### **Run a Jupyter Notebook server with your existing lakeFS Enterprise Server**

   If you have already [installed lakeFS](https://docs.lakefs.io/deploy/) or are utilizing [lakeFS cloud](https://lakefs.cloud/), all you need to run is the Jupyter notebook server:

   ```bash
   docker compose up
   ```

   Once you've finished, run the following to remove all the containers: 

   ```bash
   docker compose down
   ```

   ### **Don't have a lakeFS Enterprise Server or Object Store?**

   If you want to provision a lakeFS Enterprise server as well as MinIO for your object store, plus Jupyter then first login to [Treeverse Dockerhub](https://hub.docker.com/u/treeverse) by using the granted token so lakeFS Enterprise proprietary image can be retrieved:

   ```bash
   docker login -u externallakefs
   ```

   Copy the lakeFS license file to "lakeFS-samples/01_standalone_examples/lakefs-mount-demo" folder,
   then change lakeFS license file name and installation ID in the following command and run the command to bring up the full stack:
   ```bash
   LAKEFS_LICENSE_FILE_NAME=license-org-name-installation-id.token LAKEFS_INSTALLATION_ID=installation-id docker compose --profile local-lakefs-enterprise up
   ```

3. Copy the Everest binary for Linux x86_64 OS on your laptop inside 

   "lakeFS-samples/01_standalone_examples/lakefs-mount-demo" folder.

## URLs and login details

* JupyterLab UI http://localhost:8892/
* lakeFS Enterprise (if provisioned) http://localhost:8084/ (`AKIAIOSFOLKFSSAMPLES` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`)
* MinIO (if provisioned) http://localhost:9005/ (`minioadmin`/`minioadmin`)

## Demo Instructions

Demo includes following 3 notebooks. Open any notebook from the JupyterLab UI and follow the instructions.
1. "lakeFS Mount Demo" notebook demonstrates how to mount lakeFS datasets on laptop or server as local filesystem.
1. "lakeFS Mount Demo with Git Integration" notebook demonstrates lakeFS Mount feature as well as how it integrates with Git. In this demo, Git is used to version control your code while lakeFS is used to version control your data and model.
1. "lakeFS Hugging Face Mount Demo" notebook demonstrates lakeFS Mount feature but uses Hugging Face dataset.
