from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from airflow.models import Variable
import sys
import time

repo = Variable.get("repo")
newBranch = sys.argv[1]
newPath = Variable.get("newPath")
fileName = Variable.get("fileName")
lakefsAccessKey = Variable.get("lakefsAccessKey")
lakefsSecretKey = Variable.get("lakefsSecretKey")
lakefsEndPoint = Variable.get("lakefsEndPoint")

# S3A Gateway configuration
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", lakefsAccessKey)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", lakefsSecretKey)
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", lakefsEndPoint)
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

# Reading data by using S3A Gateway
dataPath = f"s3a://{repo}/{newBranch}/{fileName}"
df = spark.read.csv(dataPath)

# Partition the data and write to new branch by using S3A Gateway
newDataPath = f"s3a://{repo}/{newBranch}/{newPath + '_c4'}"
df.write.partitionBy("_c4").mode("overwrite").csv(newDataPath)

#time.sleep(30)