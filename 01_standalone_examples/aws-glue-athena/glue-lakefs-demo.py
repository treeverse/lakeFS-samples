## Change your lakeFS credentials
lakefsEndPoint = '<lakeFS Endpoint URL>' # e.g. 'https://username.aws_region_name.lakefscloud.io'
lakefsAccessKey = '<lakeFS Access Key>'
lakefsSecretKey = '<lakeFS Secret Key>'

## Change repo and production branch name
repo = "my-repo"
productionBranch = "main"

## Versioning Information
stagingBranch = "staging"
dev1Branch = "development1"
dev2Branch = "development2"
dev3Branch = "development3"
landingPath = "glue-landing"
rawPath = "glue-raw"
normalizedPath = "glue-normalized"


## Working with the lakeFS Python client API
import lakefs_client
from lakefs_client import models
from lakefs_client.client import LakeFSClient
# lakeFS credentials and endpoint
configuration = lakefs_client.Configuration()
configuration.username = lakefsAccessKey
configuration.password = lakefsSecretKey
configuration.host = lakefsEndPoint
client = LakeFSClient(configuration)


## S3A Gateway configuration
from pyspark.context import SparkContext
from awsglue.context import GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key",lakefsSecretKey)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint",lakefsEndPoint)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key",lakefsAccessKey)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access","true")


## Data landing
data = [('James','Smith','M',3000), ('Anna','Rose','F',4100),
  ('Robert','Williams','M',6200)
]
columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)

# Write to Production landing zone
dataPath = f"s3a://{repo}/{productionBranch}/{landingPath}"
df.write.format("parquet").mode("overwrite").save(dataPath)

client.commits.commit(
    repository=repo,
    branch=productionBranch,
    commit_creation=models.CommitCreation(
        message='Data came to landing zone',
        metadata={'using': 'python_api'}))
        

## Zero-Copy Prod landing files to Staging and multiple Dev environments.
client.branches.create_branch(
    repository=repo,
    branch_creation=models.BranchCreation(
        name=stagingBranch,
        source=productionBranch))
        
client.branches.create_branch(
    repository=repo,
    branch_creation=models.BranchCreation(
        name=dev1Branch,
        source=productionBranch))

client.branches.create_branch(
    repository=repo,
    branch_creation=models.BranchCreation(
        name=dev2Branch,
        source=productionBranch))
        
client.branches.create_branch(
    repository=repo,
    branch_creation=models.BranchCreation(
        name=dev3Branch,
        source=productionBranch))
     

## Production Glue ETL job which reads from Landing and writes to Raw Bucket
source_branch_name = productionBranch
etl_branch_name = productionBranch+"-raw-etl"

# Create ETL branch: Doesn't impact production data
client.branches.create_branch(
    repository=repo,
    branch_creation=models.BranchCreation(
        name=etl_branch_name,
        source=source_branch_name))
        
# Read from Landing zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{landingPath}"
df = spark.read.parquet(dataPath)

# ETL Task
df2 = df.withColumn("RawColumn", df.salary*0.3)

# Write to Raw zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{rawPath}"
df2.write.format("parquet").mode("overwrite").save(dataPath)

client.commits.commit(
    repository=repo,
    branch=etl_branch_name,
    commit_creation=models.CommitCreation(
        message='Production Glue ETL job which reads from Landing and writes to Raw Bucket',
        metadata={'using': 'python_api'}))

# Atomically promote data to main branch
client.refs.merge_into_branch(
    repository=repo,
    source_ref=etl_branch_name, 
    destination_branch=source_branch_name)


## Production Glue ETL job which reads from Raw and writes to Normalized Bucket
source_branch_name = productionBranch
etl_branch_name = productionBranch+"-normalized-etl"

# Create ETL branch
client.branches.create_branch(
    repository=repo,
    branch_creation=models.BranchCreation(
        name=etl_branch_name,
        source=source_branch_name))
        
# Read from Raw zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{rawPath}"
df = spark.read.parquet(dataPath)

# ETL Task
df2 = df.withColumn("NormalizedColumn", df.RawColumn*0.4)

# Write to Normalized zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{normalizedPath}"
df2.write.format("parquet").mode("overwrite").save(dataPath)

client.commits.commit(
    repository=repo,
    branch=etl_branch_name,
    commit_creation=models.CommitCreation(
        message='Production Glue ETL job which reads from Raw and writes to Normalized Bucket',
        metadata={'using': 'python_api'}))

# Atomically promote data to main branch
client.refs.merge_into_branch(
    repository=repo,
    source_ref=etl_branch_name, 
    destination_branch=source_branch_name)


## Staging Glue ETL job which reads from Landing and writes to Raw Bucket
source_branch_name = stagingBranch
etl_branch_name = stagingBranch+"-raw-etl"

# Create ETL branch
client.branches.create_branch(
    repository=repo,
    branch_creation=models.BranchCreation(
        name=etl_branch_name,
        source=source_branch_name))
        
# Read from Landing zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{landingPath}"
df = spark.read.parquet(dataPath)

# ETL Task
df2 = df.withColumn("RawColumn", df.salary*0.2)

# Write to Raw zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{rawPath}"
df2.write.format("parquet").mode("overwrite").save(dataPath)

client.commits.commit(
    repository=repo,
    branch=etl_branch_name,
    commit_creation=models.CommitCreation(
        message='Staging Glue ETL job which reads from Landing and writes to Raw Bucket',
        metadata={'using': 'python_api'}))

client.refs.merge_into_branch(
    repository=repo,
    source_ref=etl_branch_name, 
    destination_branch=source_branch_name)


## Staging Glue ETL job which reads from Raw and writes to Normalized Bucket
source_branch_name = stagingBranch
etl_branch_name = stagingBranch+"-normalized-etl"

# Create ETL branch
client.branches.create_branch(
    repository=repo,
    branch_creation=models.BranchCreation(
        name=etl_branch_name,
        source=source_branch_name))
        
# Read from Raw zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{rawPath}"
df = spark.read.parquet(dataPath)

# ETL Task
df2 = df.withColumn("NormalizedColumn", df.RawColumn*0.3)

# Write to Normalized zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{normalizedPath}"
df2.write.format("parquet").mode("overwrite").save(dataPath)

client.commits.commit(
    repository=repo,
    branch=etl_branch_name,
    commit_creation=models.CommitCreation(
        message='Production Glue ETL job which reads from Raw and writes to Normalized Bucket',
        metadata={'using': 'python_api'}))

client.refs.merge_into_branch(
    repository=repo,
    source_ref=etl_branch_name, 
    destination_branch=source_branch_name)


## Dev1 Glue ETL job which reads from Landing and writes to Raw Bucket
source_branch_name = dev1Branch
etl_branch_name = dev1Branch+"-raw-etl"

# Create ETL branch
client.branches.create_branch(
    repository=repo,
    branch_creation=models.BranchCreation(
        name=etl_branch_name,
        source=source_branch_name))
        
# Read from Landing zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{landingPath}"
df = spark.read.parquet(dataPath)

# ETL Task
df2 = df.withColumn("RawColumn", df.salary*0.1)

# Write to Raw zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{rawPath}"
df2.write.format("parquet").mode("overwrite").save(dataPath)

client.commits.commit(
    repository=repo,
    branch=etl_branch_name,
    commit_creation=models.CommitCreation(
        message='Dev1 Glue ETL job which reads from Landing and writes to Raw Bucket',
        metadata={'using': 'python_api'}))

client.refs.merge_into_branch(
    repository=repo,
    source_ref=etl_branch_name, 
    destination_branch=source_branch_name)


## Dev1 Glue ETL job which reads from Raw and writes to Normalized Bucket
source_branch_name = dev1Branch
etl_branch_name = dev1Branch+"-normalized-etl"

# Create ETL branch
client.branches.create_branch(
    repository=repo,
    branch_creation=models.BranchCreation(
        name=etl_branch_name,
        source=source_branch_name))
        
# Read from Raw zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{rawPath}"
df = spark.read.parquet(dataPath)

# ETL Task
df2 = df.withColumn("NormalizedColumn", df.RawColumn*0.2)

# Write to Normalized zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{normalizedPath}"
df2.write.format("parquet").mode("overwrite").save(dataPath)

client.commits.commit(
    repository=repo,
    branch=etl_branch_name,
    commit_creation=models.CommitCreation(
        message='Dev1 Glue ETL job which reads from Raw and writes to Normalized Bucket',
        metadata={'using': 'python_api'}))

client.refs.merge_into_branch(
    repository=repo,
    source_ref=etl_branch_name, 
    destination_branch=source_branch_name)


## Dev2 Glue ETL job which reads from Landing and writes to Raw Bucket
source_branch_name = dev2Branch
etl_branch_name = dev2Branch+"-raw-etl"

# Create ETL branch
client.branches.create_branch(
    repository=repo,
    branch_creation=models.BranchCreation(
        name=etl_branch_name,
        source=source_branch_name))
        
# Read from Landing zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{landingPath}"
df = spark.read.parquet(dataPath)

# ETL Task
df2 = df.withColumn("RawColumn", df.salary*0.4)

# Write to Raw zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{rawPath}"
df2.write.format("parquet").mode("overwrite").save(dataPath)

client.commits.commit(
    repository=repo,
    branch=etl_branch_name,
    commit_creation=models.CommitCreation(
        message='Dev2 Glue ETL job which reads from Landing and writes to Raw Bucket',
        metadata={'using': 'python_api'}))

client.refs.merge_into_branch(
    repository=repo,
    source_ref=etl_branch_name, 
    destination_branch=source_branch_name)


## Dev2 Glue ETL job which reads from Raw and writes to Normalized Bucket
source_branch_name = dev2Branch
etl_branch_name = dev2Branch+"-normalized-etl"

# Create ETL branch
client.branches.create_branch(
    repository=repo,
    branch_creation=models.BranchCreation(
        name=etl_branch_name,
        source=source_branch_name))
        
# Read from Raw zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{rawPath}"
df = spark.read.parquet(dataPath)

# ETL Task
df2 = df.withColumn("NormalizedColumn", df.RawColumn*0.5)

# Write to Normalized zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{normalizedPath}"
df2.write.format("parquet").mode("overwrite").save(dataPath)

client.commits.commit(
    repository=repo,
    branch=etl_branch_name,
    commit_creation=models.CommitCreation(
        message='Dev2 Glue ETL job which reads from Raw and writes to Normalized Bucket',
        metadata={'using': 'python_api'}))

client.refs.merge_into_branch(
    repository=repo,
    source_ref=etl_branch_name, 
    destination_branch=source_branch_name)


## Dev3 Glue ETL job which reads from Landing and writes to Raw Bucket
source_branch_name = dev3Branch
etl_branch_name = dev3Branch+"-raw-etl"

# Create ETL branch
client.branches.create_branch(
    repository=repo,
    branch_creation=models.BranchCreation(
        name=etl_branch_name,
        source=source_branch_name))
        
# Read from Landing zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{landingPath}"
df = spark.read.parquet(dataPath)

# ETL Task
df2 = df.withColumn("RawColumn", df.salary*0.5)

# Write to Raw zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{rawPath}"
df2.write.format("parquet").mode("overwrite").save(dataPath)

client.commits.commit(
    repository=repo,
    branch=etl_branch_name,
    commit_creation=models.CommitCreation(
        message='Dev3 Glue ETL job which reads from Landing and writes to Raw Bucket',
        metadata={'using': 'python_api'}))

client.refs.merge_into_branch(
    repository=repo,
    source_ref=etl_branch_name, 
    destination_branch=source_branch_name)


## Dev3 Glue ETL job which reads from Raw and writes to Normalized Bucket
source_branch_name = dev3Branch
etl_branch_name = dev3Branch+"-normalized-etl"

# Create ETL branch
client.branches.create_branch(
    repository=repo,
    branch_creation=models.BranchCreation(
        name=etl_branch_name,
        source=source_branch_name))
        
# Read from Raw zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{rawPath}"
df = spark.read.parquet(dataPath)

# ETL Task
df2 = df.withColumn("NormalizedColumn", df.RawColumn*0.6)

# Write to Normalized zone
dataPath = f"s3a://{repo}/{etl_branch_name}/{normalizedPath}"
df2.write.format("parquet").mode("overwrite").save(dataPath)

client.commits.commit(
    repository=repo,
    branch=etl_branch_name,
    commit_creation=models.CommitCreation(
        message='Dev3 Glue ETL job which reads from Raw and writes to Normalized Bucket',
        metadata={'using': 'python_api'}))

client.refs.merge_into_branch(
    repository=repo,
    source_ref=etl_branch_name, 
    destination_branch=source_branch_name)
