from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import sys

lakeFSBranch = sys.argv[1]

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

spark.sql(f"CREATE OR REPLACE TABLE lakefs.{lakeFSBranch}.lakefs_demo.book_sales(id int, sale_date date, book_id int, price double) USING iceberg")

spark.sql(f" \
INSERT INTO lakefs.{lakeFSBranch}.lakefs_demo.book_sales (id, sale_date, book_id, price) \
VALUES (1, DATE '2024-04-12', 1, 25.50), \
       (2, DATE '2024-04-11', 2, 17.99), \
       (3, DATE '2024-04-10', 3, 12.95), \
       (4, DATE '2024-04-13', 4, 32.00), \
       (5, DATE '2024-04-12', 5, 29.99), \
       (6, DATE '2024-03-15', 1, 23.99), \
       (7, DATE '2024-02-22', 2, 19.50), \
       (8, DATE '2024-01-10', 3, 14.95), \
       (9, DATE '2023-12-05', 4, 28.00), \
       (10, DATE '2023-11-18', 5, 27.99), \
       (11, DATE '2023-10-26', 2, 18.99), \
       (12, DATE '2023-10-12', 1, 22.50), \
       (13, DATE '2024-04-09', 3, 11.95), \
       (14, DATE '2024-03-28', 4, 35.00), \
       (15, DATE '2024-04-05', 5, 31.99), \
       (16, DATE '2024-03-01', 1, 27.50), \
       (17, DATE '2024-02-14', 2, 21.99), \
       (18, DATE '2024-01-07', 3, 13.95), \
       (19, DATE '2023-12-20', 4, 29.00), \
       (20, DATE '2023-11-03', 5, 28.99) \
")
