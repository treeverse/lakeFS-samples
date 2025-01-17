# Fast Data Loading and Reproducibility for Deep Learning Workloads with lakeFS Mount

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/oreilly-course) project.

This repository includes a Jupyter Notebook which you can run on your local machine.

## Prerequisites
* Docker installed on your local machine
* This demo requires connecting to a lakeFS Server. You can either install lakeFS Server locally (https://docs.lakefs.io/quickstart.html), or spin up for free on the lakeFS cloud (https://lakefs.cloud). 
* Watch [this video](https://www.youtube.com/watch?v=BgKuoa8LAaU) to understand the use case as well as the demo.
* [Contact lakeFS](https://lakefs.io/contact-sales/) to get the lakeFS Everest binary for Linux x86_64 OS. Download and save the binary on your laptop.

## Setup

1. Start by cloning this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/01_standalone_examples/lakefs-mount-demo
   ```

2. Run following commands to download and run Docker container which includes Python, Hugging Face datasets library, Pytorch, Jupyter Notebook and lakeFS Python client (Docker image size is around 10GB):

   ```bash
      docker build -t lakefs-mount-demo .

      docker run -d -p 8892:8888 --privileged -e NB_USER=root -e NB_UID=0 -e NB_GID=0 -e CHOWN_HOME=yes -w "/root" -v $PWD:/root --name lakefs-mount-demo lakefs-mount-demo /usr/local/bin/start-notebook.sh --allow-root  --NotebookApp.token='' --NotebookApp.password=''
   ```

3. Copy the Everest binary for Linux x86_64 OS on your laptop inside "lakeFS-samples/01_standalone_examples/lakefs-mount-demo" folder.

4. Open JupyterLab UI [http://127.0.0.1:8892/](http://127.0.0.1:8892/) in your web browser.

## Demo Instructions

1. Once you have successfully completed setup then open "lakeFS Mount Demo" notebook from JupyterLab UI and follow the instructions.
