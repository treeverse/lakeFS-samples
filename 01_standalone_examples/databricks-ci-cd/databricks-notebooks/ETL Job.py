ENVIRONMENT = getArgument('environment')
lakefsEndPoint = getArgument('lakefs_end_point')
repo_name = getArgument('lakefs_repo')
newBranch = getArgument('lakefs_branch')

if ENVIRONMENT == "prod":
    DATA_SOURCE = getArgument('data_source_storage_namespace')
elif ENVIRONMENT == "dev":
    DATA_SOURCE = f"lakefs://{repo_name}/{newBranch}/delta-tables"
print(DATA_SOURCE)

# COMMAND ----------

if ENVIRONMENT == "dev":
    branchURL = f"{lakefsEndPoint}/repositories/{repo_name}/objects?ref={newBranch}"
    print(f"lakeFS Branch URL: {branchURL}")

# COMMAND ----------

df = spark.read.format("delta").load(f"{DATA_SOURCE}/famous_people_raw")
df.write.format("delta").partitionBy("country").save(f"{DATA_SOURCE}/famous_people")
df.display()

# COMMAND ----------

#from pyspark.sql.functions import col
#df = spark.read.format("delta").load(f"{DATA_SOURCE}/category_raw")
#df_not_music = df.filter(col("category") != "music")
#df_not_music.write.format("delta").mode("overwrite").save(f"{DATA_SOURCE}/category_raw")
#df_not_music.display()