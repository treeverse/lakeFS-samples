
## Running data quality checks with lakeFS hooks

### Setup Python Flask Server for lakeFS webhooks
* Get a flask server running locally. THe following commands can be run in your terminal to get the hooks image.
  * `$ git clone https://github.com/treeverse/lakeFS-hooks.git`
  * `$ cd lakeFS-hooks/`
  * `docker build -t <lakefs-hooks-image-name> .` That is, `docker build -t lakefs-hooks .`
* lakeFS webhooks need a separate hooks server to serve the http requests from lakeFS server. 

### Setup lakeFS server and hooks server using "Everything Bagel" docker
* Get a local lakeFS instance running in a Docker container. This environment includes lakeFS and other common data tools like Spark, dbt, Trino, Hive, Minio, and Jupyter.
* As a prerequisite, Docker is required to be installed on your machine. 
* The following commands can be run in your terminal to get the Bagel running:
  * `git clone https://github.com/treeverse/lakeFS.git`
  * `cd lakeFS/deployments/compose`
* The webhooks flask server image we built in the above section needs to be included in the everything bagel `docker-compose` file. 
* Edit the `docker-compose.yaml` to add the following section. The environment variables access key id and secret access key are set to the same lakeFS credentials present in `lakefs-setup` service of the `docker-compose.yaml`.
```
  lakefs-webhooks:
    image: <lakefs-hooks-image-name>
    container_name: lakefs-hooks
    ports:
      - 5000:5000
    environment:
      - LAKEFS_SERVER_ADDRESS=http://lakefs:8000
      - LAKEFS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
      - LAKEFS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```
{.note}
Attention Mac users: In `macOS Monterey (version 12)` and above, they added a new service used for AirPlay that listens on port 5000 by default. Due to that, you may not be able to work with lakeFS hooks locally.

* Start the docker containers: `docker compose up -d`
* Once you have your Docker environment running, check out the following services:
  * **lakefs**:
    `http://localhost:8000` in your browser. The access key and secret to login are found in the `docker_compose.yaml` file in the `lakefs-setup` section.
  * **minio**:
    `http://localhost:9001` in your browser. The username and password are found in the `docker_compose.yaml` file in the `minio-setup` section.
  * **jupyter notebook**:
    `http://localhost:8888` in your browser. The password to login to notebook is found in `docker_compose.yaml` file in `notebook` section.

### Running lakeFS webhooks

* **Download demo notebook, dataset and hooks config** - The following commands can be run in a terminal to download demo notebook, sample movies dataset and hooks config file (`actions.yaml`): 
  * `cd && git clone https://github.com/treeverse/lakeFS-samples.git`
  * `cd 04-data-quality-checks-with-lakeFS-hooks`
* Copy the below files from the git repo and upload them to the jupyter notebook workspace path `http://localhost:8888/tree/work`:
  * `actions.yaml`
  * `lakeFS-hooks-demo.ipynb`
  * `movies.csv`

**- Open the `lakeFS-hooks-demo.ipynb` demo notebook and follow along the steps. 

