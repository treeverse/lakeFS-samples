# Databricks notebook source
#dbutils.widgets.text("environment", "dev")
#dbutils.widgets.text("data_source_storage_namespace", "s3://treeverse-ort-simulation-bucket/amit/data-source/delta-tables")
#dbutils.widgets.text("lakefs_repo", "amit-databricks-ci-cd-repo")
#dbutils.widgets.text("lakefs_branch", "test")

ENVIRONMENT = getArgument('environment')

if ENVIRONMENT == "prod":
    DATA_SOURCE = getArgument('data_source_storage_namespace')
elif ENVIRONMENT == "dev":
    DATA_SOURCE = f"lakefs://{getArgument('lakefs_repo')}/{getArgument('lakefs_branch')}/delta-tables"
print(DATA_SOURCE)

# COMMAND ----------

df = spark.read.format("delta").load(f"{DATA_SOURCE}/famous_people_raw")
df.write.format("delta").partitionBy("country").save(f"{DATA_SOURCE}/famous_people")
df.display()

# COMMAND ----------

# from pyspark.sql.functions import col
# df = spark.read.format("delta").load(f"{DATA_SOURCE}/category_raw")
# df_not_music = df.filter(col("category") != "music")
# df_not_music.write.format("delta").mode("overwrite").save(f"{DATA_SOURCE}/category_raw")
# df_not_music.display()