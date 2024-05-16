# Integration of lakeFS with Azure Databricks

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/oreilly-course) project.

This repository includes following Databricks Notebooks which you can run in your Databricks cluster:

1. Azure Databricks Tutorial:
* This notebook is used in this blog [Databricks and lakeFS Integration: Step-by-Step Configuration Tutorial](https://lakefs.io/blog/databricks-lakefs-integration-tutorial/). 

2. lakeFS Demo:
* Use Case: Managing the Data Lifecycle with lakeFS

3. Delta Lake Demo:
* Integration of lakeFS with Delta Lake
* Use Cases: Isolating ETL job and atomic promotion to production. Atomic rollback of Multi-Table Transactions.
* This notebook also runs deltaLakeSetup notebook internally.

4. Unstructured Data ML Demo:
* Use Case: Isolated Reproducible Unstructured Datasets for ML
* This notebook also runs unstructuredDataMLDemoSetup notebook internally.

5. Unity Catalog Integration Demo:
* Use Case: Isolated Unity Catalog schema for dev/test environment
* This notebook also runs unityCatalogIntegrationDemoSetup notebook internally.

## Prerequisites
* lakeFS installed and running on your local machine or on a server or in the cloud. If you don't have lakeFS already running then either use [lakeFS Cloud](https://demo.lakefs.io/) which provides lakeFS server on-demand with a single click or refer to [lakeFS Quickstart](https://docs.lakefs.io/quickstart/) doc.
* Databricks server with the ability to run compute clusters on top of it. 
* Configure your Databricks cluster to use lakeFS Hadoop file system in Presigned Mode. Read this blog [Databricks and lakeFS Integration: Step-by-Step Configuration Tutorial](https://lakefs.io/blog/databricks-lakefs-integration-tutorial/) or [lakeFS documentation](https://docs.lakefs.io/integrations/spark.html#hadoop-filesystem-in-presigned-mode-beta) for the configuration.
* Permissions to manage the cluster configuration, including adding libraries. 


## Setup

1. Download these notebooks from GitHub and [import it in your Databricks workspace](https://learn.microsoft.com/en-us/azure/databricks/notebooks/notebook-export-import#--import-a-notebook).


## Demo Instructions

Once you have successfully completed setup then open any notebook from Databricks UI and follow the instructions.

