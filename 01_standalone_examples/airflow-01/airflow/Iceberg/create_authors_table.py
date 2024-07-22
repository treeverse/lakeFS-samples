from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import sys

lakeFSBranch = sys.argv[1]

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

spark.sql(f"CREATE OR REPLACE TABLE lakefs.{lakeFSBranch}.lakefs_demo.authors(id int, name string) USING iceberg")

spark.sql(f' \
INSERT INTO lakefs.{lakeFSBranch}.lakefs_demo.authors (id, name) \
VALUES (1, "J.R.R. Tolkien"), (2, "George R.R. Martin"), \
       (3, "Agatha Christie"), (4, "Isaac Asimov"), (5, "Stephen King") \
')
