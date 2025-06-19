# Integration of lakeFS Iceberg REST Catalog with Trino client

## Prerequisites

* Docker installed on your local machine

## Setup

1. Start by cloning this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/01_standalone_examples/trino
   ```

2. To configure the lakeFS Iceberg connector in Trino, this sample provides a catalog properties file `lakefs.properties` that references the lakeFS iceberg connector.

* **_If you're not using lakeFS Enterprise server provided as part of the lakeFS Samples then change configuration properties for `iceberg.rest-catalog.uri`, `iceberg.rest-catalog.oauth2.server-uri` & `iceberg.rest-catalog.oauth2.credential` in this `lakefs.properties` file to match your lakeFS Enterprise environment_**

* **_Also, if you're not using the provided MinIO storage then change `s3.endpoint` (e.g. http://s3.us-east-1.amazonaws.com) and S3 credentials in this `lakefs.properties` file to match your storage_**

3. Run the following to provision the stack which includes Trino:

   ```bash
   docker compose up 
   ```

## Demo Instructions

Once you have successfully completed setup then open `iceberg-books-trino` notebook provided with lakeFS Enterprise Samples from the JupyterLab UI and follow the instructions.