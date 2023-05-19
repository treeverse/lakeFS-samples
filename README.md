# lakefs-samples
This sample repository captures a collection of notebooks, dockerized applications and code snippets that demonstrate how to use lakeFS.

> **lakeFS:** LakeFS is a popular open-source solution for managing data. It provides a consistent and scalable data management layer on top of cloud storage, such as Amazon S3, Azure Blob Storage, or Google Cloud Storage. It allows users to create and manage data in a version-controlled and immutable manner, and offers features such as data governance, data lineage, and data access controls. lakeFS is compatible with a wide range of data processing frameworks and tools.


# How to navigate?
Each directory is named after a specific lakeFS use case. And each project has it's own readme. Navigate to the use case of your interest and follow along the steps mentioned in the readme to check out a lakeFS use case.

For example, 

* `01-cross-collection-consistency-deltalake-lakefs` shows how to implement cross collection consistency on multiple delta tables using lakeFS atomic merges.

* `03-multiple-samples` includes mutliple notebooks to show integration of lakeFS with Spark, Python, Delta Lake, Airflow and Hooks.

* `04-data-quality-checks-with-lakeFS-hooks` shows how to use lakeFS webhooks to run automated data quality checks on different data branches.

* `05-create-dev-test-env-with-lakeFS-branches` shows how to use lakeFS branching feature to create dev/test data environments for ETL testing and experimentation.

* `07-ml-reproducibility-with-lakeFS` shows how to use lakeFS to reproduce ML experiments (using unstructured data) with certainty using lakeFS tags.

* `08-data-lineage` demonstrating data lineage with lakeFS.

* `10-ml-experimentation-with-lakeFS-structured-data` shows how to use lakeFS to reproduce ML experiments (using structured data) with certainty using lakeFS tags.

* `15-write-audit-publish` contains a set of Jupyter notebooks demonstrating how the Write-Audit-Publish pattern can be implemented with different technologies, including lakeFS. 


# QA
Join the [lakeFS, data practitioner slack](https://go.lakefs.io/3iyZLOh) to understand more about lakeFS use cases and get help with installing lakeFS as well! 
