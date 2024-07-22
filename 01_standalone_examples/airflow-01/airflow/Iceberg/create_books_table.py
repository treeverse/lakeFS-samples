from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import sys

lakeFSBranch = sys.argv[1]

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

spark.sql(f"CREATE OR REPLACE TABLE lakefs.{lakeFSBranch}.lakefs_demo.books(id int, title string, author_id int) USING iceberg")

spark.sql(f' \
INSERT INTO lakefs.{lakeFSBranch}.lakefs_demo.books (id, title, author_id) \
VALUES (1, "The Lord of the Rings", 1), (2, "The Hobbit", 1), \
       (3, "A Song of Ice and Fire", 2), (4, "A Clash of Kings", 2), \
       (5, "And Then There Were None", 3), (6, "Murder on the Orient Express", 3), \
       (7, "Foundation", 4), (8, "I, Robot", 4), \
       (9, "The Shining", 5), (10, "It", 5) \
')
