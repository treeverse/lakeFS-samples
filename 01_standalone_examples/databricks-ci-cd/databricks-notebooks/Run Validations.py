databricksSecretScope = getArgument('databricks_secret_scope')
lakefsEndPoint = getArgument('lakefs_end_point')
repo_name = getArgument('lakefs_repo')
newBranch = getArgument('lakefs_branch')
create_lakefs_repo_run_url = getArgument('create_lakefs_repo_run_url')
etl_job_run_url = getArgument('etl_job_run_url')

lakefsAccessKey = dbutils.secrets.get(databricksSecretScope, 'lakefs_access_key_id')
lakefsSecretKey = dbutils.secrets.get(databricksSecretScope, 'lakefs_secret_access_key')
sourceBranch = "main"
DATA_SOURCE = f"lakefs://{repo_name}/{newBranch}/delta-tables"

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

repo = lakefs.Repository(repo_name, client=clt)
branchNew = repo.branch(newBranch)
ref = branchNew.commit(message='Commit ETL job changes',
                 metadata={'::lakefs::Create lakeFS Repo and Import Data Notebook::url[url:ui]': create_lakefs_repo_run_url,
                           '::lakefs::ETL Job Notebook::url[url:ui]': etl_job_run_url})
commitURL = f"{lakefsEndPoint}/repositories/{repo_name}/commits/{ref.get_commit().id}"
print(commitURL)

# COMMAND ----------

df_category = spark.read.format("delta").load(f"{DATA_SOURCE}/category_raw")
df_category.display()

# COMMAND ----------

df_famous_people = spark.read.format("delta").load(f"{DATA_SOURCE}/famous_people")
df_famous_people.groupby("category").count().display()

# COMMAND ----------

# Check number of categories
number_of_categories = df_famous_people.groupby("category").count().count()
if number_of_categories == df_category.count():
    dbutils.notebook.exit("Success")
else:
    dbutils.notebook.exit(f"Referential integrity issue. Number of categories in 'famous_people' table are {number_of_categories} while number of categories in parent 'category_raw' table are {df_category.count()}.")