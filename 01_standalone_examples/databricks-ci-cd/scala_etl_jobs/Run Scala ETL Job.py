# COMMAND ----------

# MAGIC %scala
# MAGIC val ENVIRONMENT = dbutils.widgets.get("environment")
# MAGIC val repo_name = dbutils.widgets.get("lakefs_repo")
# MAGIC val newBranch = dbutils.widgets.get("lakefs_branch")
# MAGIC val data_source_storage_namespace = dbutils.widgets.get("data_source_storage_namespace")

# COMMAND ----------

# MAGIC %scala
# MAGIC import example.etl_jobs
# MAGIC val test = etl_jobs.main(Array(ENVIRONMENT,repo_name,newBranch,data_source_storage_namespace))

# COMMAND ----------


