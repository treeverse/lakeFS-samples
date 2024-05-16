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
time.sleep(2)

while not importer.status().completed and importer.status().error is None:
    time.sleep(2)
    status = importer.status()
    print(status)

if importer.status().completed:
    print(f"\nImported a total of {importer.status().ingested_objects} objects into branch {sourceBranch}")

if importer.status().error:
    raise Exception(status.error)

# COMMAND ----------

branchNew = repo.branch(newBranch).create(source_reference=sourceBranch, exist_ok=True)
branchURL = f"{lakefsEndPoint}/repositories/{repo_name}/objects?ref={newBranch}"
print(f"lakeFS Branch URL: {branchURL}")

# COMMAND ----------

dbutils.notebook.exit(branchURL)