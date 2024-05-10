# Databricks notebook source
#dbutils.widgets.text("databricks_secret_scope", "demos")
#dbutils.widgets.text("lakefs_end_point", "https://treeverse.us-east-1.lakefscloud.io")
#dbutils.widgets.text("lakefs_repo", "amit-databricks-ci-cd-repo")
#dbutils.widgets.text("lakefs_repo_storage_namespace", "s3://treeverse-ort-simulation-bucket/amit")
#dbutils.widgets.text("lakefs_branch", "test")
#dbutils.widgets.text("data_source_storage_namespace", "s3://treeverse-ort-simulation-bucket/amit/data-source/delta-tables")

databricksSecretScope = getArgument('databricks_secret_scope')
lakefsEndPoint = getArgument('lakefs_end_point')
repo_name = getArgument('lakefs_repo')
storageNamespace = getArgument('lakefs_repo_storage_namespace') + '/' + repo_name
newBranch = getArgument('lakefs_branch')
importSource = getArgument('data_source_storage_namespace')

lakefsAccessKey = dbutils.secrets.get(databricksSecretScope, 'lakefs_access_key_id')
lakefsSecretKey = dbutils.secrets.get(databricksSecretScope, 'lakefs_secret_access_key')
sourceBranch = "main"
importDestination = ""

# COMMAND ----------

import lakefs
from lakefs.client import Client

clt = Client(
    host=lakefsEndPoint,
    username=lakefsAccessKey,
    password=lakefsSecretKey,
)

print("Verifying lakeFS credentials")
print(clt.version)
print("lakeFS credentials verified")

# COMMAND ----------

repo = lakefs.Repository(repo_name, client=clt).create(storage_namespace=storageNamespace, default_branch=sourceBranch, exist_ok=True)
branchMain = repo.branch(sourceBranch)
print(repo)

# COMMAND ----------

import time

importer = branchMain.import_data(commit_message="import objects", metadata={"key": "value"}) \
    .prefix(importSource, destination=importDestination)

importer.start()
status = importer.status()
print(status)

while not status.completed and status.error is None:
    time.sleep(2)
    status = importer.status()
    print(status)

if status.error:
    raise Exception(status.error)
    
print(f"\nImported a total of {status.ingested_objects} objects into branch {sourceBranch}")

# COMMAND ----------

branchNew = repo.branch(newBranch).create(source_reference=sourceBranch)
print(f"{newBranch} ref:", branchNew.get_commit().id)