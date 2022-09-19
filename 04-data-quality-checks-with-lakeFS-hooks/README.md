
## Running data quality checks with lakeFS hooks

### Setup lakeFS server and hooks server using "Everything Bagel" docker
* Get a local lakeFS instance running in a Docker container. This environment includes lakeFS and other common data tools like Spark, dbt, Trino, Hive, Minio, and Jupyter.
* As a prerequisite, Docker is required to be installed on your machine. 
* The following commands can be run in your terminal to get the Bagel running:
  * `git clone https://github.com/treeverse/lakeFS.git`
  * `cd lakeFS/deployments/compose`
* Edit the `docker-compose.yaml` to include lakeFS webhooks server to the container
```  
lakefs-webhooks:
    image: python
    container_name: lakefs-hooks-server
    ports:
      - 5000:5000
    environment:
      - FLASK_APP=server.py
    command: ["python3", "-m", "flask", "run"]
```
* Start the docker containers: `docker compose up -d`
* Once you have your Docker environment running, check out the following services:
  * **lakefs**:
    `http://localhost:8000` in your browser. The access key and secret to login are found in the `docker_compose.yaml` file in the `lakefs-setup` section.
  * **minio**:
    `http://localhost:9001` in your browser. The username and password are found in the `docker_compose.yaml` file in the `minio-setup` section.
  * **jupyter notebook**:
    `http://localhost:8888` in your browser.

### Running lakeFS webhooks

* **Download demo notebook, dataset and hooks config** - The following commands can be run in a terminal to download demo notebook, sample movies dataset and hooks config file (`actions.yaml`): 
  * `cd && git clone https://github.com/treeverse/lakeFS-samples.git`
  * `cd 04-data-quality-checks-with-lakeFS-hooks`
* Copy the below files from the git repo and upload them to the jupyter notebook workspace path `http://localhost:8888/tree/work`:
  * `actions.yaml`
  * `lakeFS-hooks-demo.ipynb`
  * `movies.csv`

**- Open the `lakeFS-hooks-demo.ipynb` demo notebook and follow along the steps. 

