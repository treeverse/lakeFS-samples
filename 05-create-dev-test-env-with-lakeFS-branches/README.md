
## Create Dev/Test data environments with lakeFS branches

### Setup lakeFS server using "Everything Bagel" docker
* Get a local lakeFS instance running in a Docker container. This environment includes lakeFS and other common data tools like Spark, dbt, Trino, Hive, Minio, and Jupyter.
* As a prerequisite, Docker is required to be installed on your machine. 
* The following commands can be run in your terminal to get the Bagel running:
```bash
  git clone https://github.com/treeverse/lakeFS.git && cd lakeFS/deployments/compose && docker compose up -d
```

* Once you have your Docker environment running, check out the following services:
  * **lakefs**:
    `http://localhost:8000` in your browser. The access key and secret to login are found in the `docker_compose.yaml` file in the `lakefs-setup` section.
  * **minio**:
    `http://localhost:9001` in your browser. The username and password are found in the `docker_compose.yaml` file in the `minio-setup` section.
  * **jupyter notebook**:
    `http://localhost:8888` in your browser. The password to login to notebook is found in `docker_compose.yaml` file in `notebook` section.

### Clone lakeFS-samples repo to play around with lakeFS use cases
* Run the following commands in your terminal to clone lakeFS-samples repo:
```bash
  git clone https://github.com/treeverse/lakeFS-samples.git && cd lakeFS-samples/05-create-dev-test-env-with-lakeFS-branches
```

* Upload the `lakeFS-samples/05-create-dev-test-env-with-lakeFS-branches/Create-DevTest-Env-with-lakeFS-branches.ipynb` to your jupyter notebook workspace path `http://localhost:8888/tree/work` 

* Open the `Create-DevTest-Env-with-lakeFS-branches.ipynb` demo notebook and follow along the steps. 

