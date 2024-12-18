# Fast Data Loading for Deep Learning Workloads with lakeFS Mount

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/oreilly-course) project.

This repository includes a Jupyter Notebook which you can run on your local machine.

## Prerequisites
* Docker installed on your local machine
* This demo requires connecting to a lakeFS Server. You can either install lakeFS Server locally (https://docs.lakefs.io/quickstart.html), or spin up for free on the lakeFS cloud (https://lakefs.cloud). 
* Watch [this video](https://www.youtube.com/watch?v=BgKuoa8LAaU) to understand the use case as well as the demo.
* [Contact lakeFS](https://lakefs.io/contact-sales/) to get the lakeFS Everest binary. Download and save the binary on your Mac laptop.

## Setup

1. Start by cloning this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/01_standalone_examples/deep-learning-with-mount
   ```

2. Run following commands to download and run Docker container which includes Python, TenserFlow, Jupyter Notebook and lakeFS Python client (Docker image size is around 6GB):

   ```bash
      docker build -t lakefs-deep-learning-demo .

      docker run -d -p 8892:8888 --user root -e GRANT_SUDO=yes -v $PWD:/home/jovyan -v $PWD/jupyter_notebook_config.py:/home/jovyan/.jupyter/jupyter_notebook_config.py -v $PWD/../../data/alpaca_training_imgs:/home/jovyan/alpaca_training_imgs --name lakefs-deep-learning-demo lakefs-deep-learning-demo
   ```

3. Open JupyterLab UI [http://127.0.0.1:8892/](http://127.0.0.1:8892/) in your web browser.

## Demo Instructions

Once you have successfully completed setup then open "Mount Demo" notebook from JupyterLab UI and follow the instructions.
