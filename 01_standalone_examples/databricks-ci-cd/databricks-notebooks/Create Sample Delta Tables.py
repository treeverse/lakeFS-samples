dataSourceStorageNamespace = getArgument('data_source_storage_namespace')

# COMMAND ----------

data = [
   (100,'intelligence'),
   (200,'music'),
   (300,'entertainment'),
   (400,'professional athlete'),
]
columns = ["category_id", "category"]

df = spark.createDataFrame(data=data, schema = columns)
df.write.format("delta").mode("overwrite").save(f"{dataSourceStorageNamespace}/category_raw")
df.display()

# COMMAND ----------

data = [
   ('James','Bond','England',100),
   ('Robbie','Williams','England',200),
   ('Hulk','Hogan','USA',300),
   ('Mister','T','USA',300),
   ('Rafael','Nadal','Spain',400),
   ('Paul','Haver','Belgium',200),
]
columns = ["firstname", "lastname", "country", "category"]

df = spark.createDataFrame(data=data, schema = columns)
df.write.format("delta").mode("overwrite").save(f"{dataSourceStorageNamespace}/famous_people_raw")
df.display()