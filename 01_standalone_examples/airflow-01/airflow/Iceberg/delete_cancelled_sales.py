from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import sys

lakeFSBranch = sys.argv[1]

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

spark.sql(f" \
DELETE FROM lakefs.{lakeFSBranch}.lakefs_demo.book_sales \
WHERE id IN (10, 15, 2, 1, 6); \
")
