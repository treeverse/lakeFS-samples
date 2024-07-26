## Demo Instructions for Scala ETL Jobs
#### Additional Setup

1. This demo will compile Scala programs and will build a JAR file. Demo will upload the JAR file to AWS S3. Demo will also create a Databricks cluster on the fly and will install the JAR file and lakeFS libraries. So, additional setup steps are required.

1. Remove **pr_commit_run_databricks_etl_job.yml** file from **.github/workflows** folder in your Git repo.

1. Upload **pr_commit_run_scala_etl_jobs.yml** file in **lakeFS-samples/01_standalone_examples/databricks-ci-cd/scala_etl_jobs** folder to **.github/workflows** folder in your Git repo.

1. Create folder **scala_etl_jobs** in your Git repo.

1. Upload all files in **lakeFS-samples/01_standalone_examples/databricks-ci-cd/scala_etl_jobs** folder to **scala_etl_jobs** folder in your Git repo.

1. Add following secrets in your Git repo:
* AWS Access Key so GitHub Action can upload JAR file to S3 and Databricks cluster can access S3 bucket:

      AWS_ACCESS_KEY

* AWS Secret Key so GitHub Action can upload JAR file to S3 and Databricks cluster can access S3 bucket:

      AWS_SECRET_KEY

* LakeFS Access Key so Databricks cluster can access lakeFS server:

      LAKEFS_ACCESS_KEY

* LakeFS Secret Key so Databricks cluster can access lakeFS server:

      LAKEFS_SECRET_KEY

7. Add following variables in your Git repo to upload JAR file to S3:
* AWS Region e.g. us-east-1:

      AWS_REGION

* AWS S3 bucket name e.g. sample-jars:

      AWS_BUCKET_FOR_JARS

* Root folder name in S3 bucket used above e.g. uploaded-jars:

      AWS_BUCKET_ROOT_FOLDER_FOR_JARS

#### Demo Instructions
1. Create a new branch in your Git repository. Select the newly created branch.
1. Remove the comment from the last 4 lines of code in **etl_jobs.scala** inside the **scala_etl_jobs/src/main/scala/example** folder and Commit your changes.
1. Go to the **Pull requests** tab in your Git repo, create Pull Request.
1. Go to the **Actions** tab in your Git repo. Git Action will start running automatically and validation checks will fail.
1. Go back to the **Code** tab in your Git repo and select the branch created in 1st step. Comment back the last 4 lines of code in **etl_jobs.scala** and Commit your changes.
1. Go back to the **Actions** tab in your Git repo. Git Action will start running again and validation checks will pass this time.
