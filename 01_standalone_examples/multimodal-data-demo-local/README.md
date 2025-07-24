# Multimodal Data Demo - Run Locally

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/oreilly-course) project.

This repository includes a Jupyter Notebook which you can run on your local machine. The notebook demonstrates ML Data Version Control and Reproducibility of Multimodal Data.

In the ever-evolving landscape of machine learning (ML), data stands as the cornerstone upon which triumphant models are built. However, as ML projects expand and encompass larger and different types of datasets, the challenge of efficiently managing and controlling multimodal data at scale becomes more pronounced.

* Breaking Down Conventional Approaches:
1. The Copy/Paste Predicament: In the world of data science, it's commonplace for data scientists to extract subsets of data to their local environments for model training. This method allows for iterative experimentation, but it introduces challenges that hinder the seamless evolution of ML projects.
2. Reproducibility Constraints: Traditional practices of copying and modifying data locally lack the version control and audit-ability crucial for reproducibility. Iterating on models with various data subsets becomes a daunting task.
3. Inefficient Data Transfer: Regularly shuttling data between the central repository and local environments strains resources and time, especially when choosing different subsets of data for each training run.

* In this demo, we will demonstrate:
1. How to use lakeFS to version control your data when working with your data locally.
3. We will be leveraging the technology stack of: MinIO, Iceberg, PyTorch and MLflow


## Prerequisites
* Docker installed on your local machine

## Setup

1. Start by cloning this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples
   cd lakeFS-samples/01_standalone_examples/multimodal-data-demo-local
   ```

2. Login to [Treeverse Dockerhub](https://hub.docker.com/u/treeverse) by using the granted token so lakeFS Enterprise proprietary image can be retrieved. [Contact Sales](https://lakefs.io/contact-sales/) to get the token for lakeFS Enterprise:

   ```bash
   docker login -u externallakefs
   ```


3. Run the following to provision the full stack which includes lakeFS Enterprise, MinIO, Python, Spark, Jupyter Notebook, JDK, Hadoop binaries and lakeFS Python client

   ```bash
   docker compose up
   ```

   If any of the port numbers (8893, 4043, 5002, 8003, 9002 and 9003) are already in use then change the port numbers in docker-compose.yml file to any available ports.

4. Open JupyterLab UI [http://127.0.0.1:8893/](http://127.0.0.1:8893/) in your web browser.

## Demo Instructions

1. Once you have successfully completed setup then open "Image Segmentation Iceberg" notebook from JupyterLab UI and follow the instructions.
