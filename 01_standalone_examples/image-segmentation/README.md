# Image Segmentation Demo

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/oreilly-course) project.

This repository includes a Jupyter Notebook which you can run on your local machine. The notebook demonstrates ML Data Version Control and Reproducibility at Scale.

In the ever-evolving landscape of machine learning (ML), data stands as the cornerstone upon which triumphant models are built. However, as ML projects expand and encompass larger and more complex datasets, the challenge of efficiently managing and controlling data at scale becomes more pronounced.

* Breaking Down Conventional Approaches:
1. The Copy/Paste Predicament: In the world of data science, it's commonplace for data scientists to extract subsets of data to their local environments for model training. This method allows for iterative experimentation, but it introduces challenges that hinder the seamless evolution of ML projects.
2. Reproducibility Constraints: Traditional practices of copying and modifying data locally lack the version control and audit-ability crucial for reproducibility. Iterating on models with various data subsets becomes a daunting task.
3. Inefficient Data Transfer: Regularly shuttling data between the central repository and local environments strains resources and time, especially when choosing different subsets of data for each training run.
4. Limited Compute Power: Operating within a local environment hampers the ability to harness the full power of parallel computing, as well as the distributed prowess of systems like Apache Spark.

* In this demo, we will demonstrate:
1. How to use lakeFS to version control your data when working with your data locally.
2. How to use lakeFS without the need to copy data and train your model at scale directly on the Cloud.
3. We will be leveraging the technology stack of: AWS S3, Databricks Delta Lake, PyTorch and MLflow


## Prerequisites
* Docker installed on your local machine
* This demo requires connecting to a lakeFS Server. You can spin up lakeFS Server for free on the lakeFS cloud (https://lakefs.cloud). 

## Setup

1. Start by cloning this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/01_standalone_examples/image-segmentation
   ```

2. Run following commands to build and run Docker container which includes Python, Spark, Jupyter Notebook and required Python packages (Docker image size is around 10GB):

   ```bash
      docker build -t lakefs-image-segmentation-demo .

      docker run -d -p 8889:8888 -p 4041:4040 -p 5001:5000 --user root -e GRANT_SUDO=yes -v $PWD:/home/jovyan -v $PWD/jupyter_notebook_config.py:/home/jovyan/.jupyter/jupyter_notebook_config.py --name lakefs-image-segmentation-demo lakefs-image-segmentation-demo
   ```

If any of the port numbers (8889, 4041 and 5001) are already in use then change the port numbers to any available ports.

3. Open JupyterLab UI [http://127.0.0.1:8889/](http://127.0.0.1:8889/) in your web browser.

## Demo Instructions

1. Once you have successfully completed setup then open "Image Segmentation" notebook from JupyterLab UI and follow the instructions.
2. If you want to run same notebook on the Databricks cluster then import "Image Segmentation" and "ImageSegmentationSetup" notebooks to your Databricks environment.
