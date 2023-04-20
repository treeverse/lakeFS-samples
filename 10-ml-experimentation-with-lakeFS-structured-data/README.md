
## ML Experimentation for Structured Data: Achieve reproducibility of experiments with lakeFS

In this tutorial, you will learn how to version your ML training data, model artifacts, metrics and your training code together with lakeFS.

### Prerequisites:
1. Have Docker installed on your machine.
2. We will be using [Wine-Quality-Dataset](https://archive.ics.uci.edu/ml/datasets/wine+quality) for the multi class classification models.

### Setup lakeFS server using "Everything Bagel" docker
* Get a local lakeFS instance running in a Docker container. This environment includes lakeFS and other common data tools like Spark, dbt, Trino, Hive, Minio, and Jupyter.
* The following commands can be run in your terminal to get the Bagel running:


   ```bash
   git clone https://github.com/treeverse/lakeFS.git && cd lakeFS/deployments/compose && docker compose up -d
   ```

Once you have your Docker environment running, check out the following services:
* **lakefs**:
  ``` http://localhost:8000``` in your browser. The access key and secret to login are found in the `docker_compose.yaml` file in the `lakefs-setup` section.
* **minio**:
  ``` http://localhost:9001``` in your browser. The username and password are found in the `docker_compose.yaml` file in the `minio-setup` section.
* **jupyter notebook**:
  ```http://localhost:8888``` in your browser. The password to login to notebook is found in `docker_compose.yaml` file in `notebook` section.

### Download demo Jupyter notebook
* Run the following commands to access the demo notebooks.
  ```bash 
  git clone https://github.com/treeverse/lakeFS-samples.git && cd 10-ml-experimentation-with-lakeFS-structured-data
* Copy the below files from the git repo and upload them to the jupyter notebook workspace path `http://localhost:8888/tree/work`:
  ``` wine-quality-prediction-experimentation.ipynb```
  ``` wine-quality-white-and-red.csv```

####  Open the `wine-quality-prediction-experimentation.ipynb` notebook and follow along the steps. 

