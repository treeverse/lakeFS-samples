# lakeFS-samples-ci-cd

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/oreilly-course) project.

**This sample captures a collection of Databricks notebooks and GitHub Action code that demonstrate how to run Databricks ETL jobs in an isolated environment by using lakeFS.**

## Prerequisites
* lakeFS installed and running on a server or in the cloud. If you don't have lakeFS already running then either use [lakeFS Cloud](https://demo.lakefs.io/) which provides free lakeFS server on-demand with a single click or refer to [lakeFS Quickstart](https://docs.lakefs.io/quickstart/) doc.
* Databricks server with the ability to run compute clusters on top of it. 
* Configure your Databricks cluster to use lakeFS Hadoop file system. Read this blog [Databricks and lakeFS Integration: Step-by-Step Configuration Tutorial](https://lakefs.io/blog/databricks-lakefs-integration-tutorial/) or [lakeFS documentation](https://docs.lakefs.io/integrations/spark.html#lakefs-hadoop-filesystem) for the configuration.
* Permissions to manage the cluster configuration, including adding libraries. 
* GitHub account. 

## Setup

1. Create [Databricks personal access token](https://docs.databricks.com/en/dev-tools/auth/pat.html).


1. Create Databricks secret scope e.g. **demos** or use an existing secret scope. Add following secrets in that secret scope by following [Secret management docs](https://docs.databricks.com/en/security/secrets/index.html): 

       lakefs_access_key_id e.g. 'AKIAIOSFOLKFSSAMPLES'

       lakefs_secret_access_key e.g. 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'

    You can use following Databricks commands to create secrets:
   ```bash
   databricks secrets put-secret --json '{
     "scope": "demos",
     "key": "lakefs_access_key_id",
     "string_value": "AKIAIOSFOLKFSSAMPLES"
   }'

   databricks secrets put-secret --json '{
     "scope": "demos",
     "key": "lakefs_secret_access_key",
     "string_value": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
   }'
   ```

1. Create a Git repository. It can be named **lakeFS-samples-ci-cd**.

1. Clone this repository:

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/01_standalone_examples/databricks-ci-cd
   ```

1. Create folders **.github/workflows** and **databricks-notebooks** in your Git repo.

1. Upload **pr_commit_run_databricks_etl_job.yml** file in **lakeFS-samples/01_standalone_examples/databricks-ci-cd/.github/workflows** folder to **.github/workflows** folder in your Git repo.

1. Upload all files in **lakeFS-samples/01_standalone_examples/databricks-ci-cd/databricks-notebooks** folder to **databricks-notebooks** folder in your Git repo.

1. Add following secrets in your Git repo by following [Creating secrets for a repository docs](https://docs.github.com/en/actions/security-guides/using-secrets-in-github-actions#creating-secrets-for-a-repository). This is the Databricks token created in 1st step above. If you copy & paste the secret name then verify that there are no spaces before and after the secret name.

       DATABRICKS_TOKEN


1. Add following variables in your Git repo by following [Creating configuration variables for a repository docs](https://docs.github.com/en/actions/learn-github-actions/variables#creating-configuration-variables-for-a-repository):
* Variable to store your [Databricks host name or URL](https://docs.databricks.com/en/workspace/workspace-details.html#workspace-instance-names-urls-and-ids) e.g. https://cust-success.cloud.databricks.com

      DATABRICKS_HOST

* Variable to store your [Databricks Cluster ID](https://docs.databricks.com/en/workspace/workspace-details.html#cluster-url-and-id) e.g. 1115-164516-often242

      DATABRICKS_CLUSTER_ID

* Variable to store your [Databricks Workspace Folder path](https://docs.databricks.com/en/workspace/workspace-details.html#folder-id) e.g. /Shared/lakefs_demos/ci_cd_demo or /Users/me@example.com/MyFolder/lakefs_demos/ci_cd_demo

      DATABRICKS_WORKSPACE_NOTEBOOK_PATH

* Variable to store your Databricks Secret Scope created in 2nd step e.g. demos

      DATABRICKS_SECRET_SCOPE

* Variable to store your lakeFS End Point e.g. https://company.region.lakefscloud.io

      LAKEFS_END_POINT

* Variable to store your lakeFS repository name (which will be created by this demo) e.g. databricks-ci-cd-repo

      LAKFES_REPO_NAME

* Variable to store the storage namespace for the lakeFS repo. It is a location in the underlying storage where data for lakeFS repository will be stored. e.g. s3://example

      LAKEFS_REPO_STORAGE_NAMESPACE

* Variable to store the storage namespace where Delta tables created by this demo will be stored e.g. s3://data-source/delta-tables. Do NOT use the same storage namespace as above.

  If it is not there then create Databricks [External Location](https://docs.databricks.com/en/sql/language-manual/sql-ref-external-locations.html) to write to s3://data-source URL and you should have **READ FILES** and **WRITES FILES** [premissions on and External Location](https://docs.databricks.com/en/connect/unity-catalog/manage-external-locations.html#grant-permissions-on-an-external-location)

      DATA_SOURCE_STORAGE_NAMESPACE

## Demo Instructions

1. Create a new branch in your Git repository. Select newly created branch.
1. Remove the comment from the last 5 lines of code in **ETL Job.py** inside **databricks-notebooks** folder and Commit your changes.
1. Go to **Pull requests** tab in your Git repo, create Pull Request.
1. Go to **Actions** tab in your Git repo. Git Action will start running automaically and validation checks will fail.
1. Go back to **Code** tab in your Git repo and select the branch created in 1st step. Comment back the last 5 lines of code in **ETL Job.py** and Commit your changes.
1. Go back to **Actions** tab in your Git repo. Git Action will start running again and validation checks will pass this time.

## Useful Information

1. Databricks [Continuous integration and delivery using GitHub Actions](https://docs.databricks.com/en/dev-tools/ci-cd/ci-cd-github.html).
1. Information on how to [run Databricks notebooks from GitHub Action](https://github.com/databricks/run-notebook/tree/main).
1. See [action.yml](https://github.com/databricks/run-notebook/blob/main/action.yml) for the latest interface and docs for databricks/run-notebook.
1. [Databricks REST API reference](https://docs.databricks.com/api/workspace/introduction).
1. GitHub [Events that trigger workflows](https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows).
1. GitHub [Webhook events and payloads](https://docs.github.com/en/webhooks/webhook-events-and-payloads).
1. GitHub [Payloads for Pull Request](https://docs.github.com/en/webhooks/webhook-events-and-payloads?actionType=closed#pull_request).

## Additional Useful GitHub Action Code

1. Code to run the Action workflow only if any file changes in a specific folder e.g. databricks-notebooks. So, changing README file will not run the workflow:

   ```bash
   name: Run Databricks ETL jobs in an isolated environment by using lakeFS

   on:
      pull_request:
         paths:
            - 'databricks-notebooks/**'
   ```

1. If you use Scala for Databricks notebooks then this is the step to build Scala job:

   ```bash
      - name: Build Scala job
        run:  |
          cd etl_jobs
          sbt assembly
          ls -lh /home/runner/work/image-segmentation-repo/image-segmentation-repo/etl_jobs/target/scala-2.12/transform-assembly-0.1.0-SNAPSHOT.jar
   ```

1. Upload a file to S3 e.g. upload JAR file built in the previous step:

   ```bash
      - name: Upload JAR to S3
        uses: hkusu/s3-upload-action@v2
        id: S3
        with:
          aws-access-key-id: ${{secrets.AWS_ACCESS_KEY}}
          aws-secret-access-key: ${{secrets.AWS_SECRET_KEY}}
          aws-region: 'us-east-1'
          aws-bucket: "treeverse-ort-simulation-bucket"
          bucket-root: "amit"
          destination-dir: "jars/pr-${{ github.event.number }}"
          file-path: "/home/runner/work/image-segmentation-repo/image-segmentation-repo/etl_jobs/target/scala-2.12/transform-assembly-0.1.0-SNAPSHOT.jar"
   ```

1. Upload a file to DBFS (Databricks FS) e.g. upload JAR file built in the previous step:

   ```bash
      - name: Upload JAR to DBFS
        uses: databricks/upload-dbfs-temp@v0
        with:
          local-path: /home/runner/work/image-segmentation-repo/image-segmentation-repo/etl_jobs/target/scala-2.12/transform-assembly-0.1.0-SNAPSHOT.jar
        id: upload_jar
      - name: Get JAR location
        run: |
            echo "JAR location: ${{ steps.upload_jar.outputs.dbfs-file-path }}"
   ```

1. Create a new Databricks cluster instead of using an existing cluster, install libraries and trigger Scala job:

   ```bash
      - name: Trigger Databricks Run Scala ETL Job
        uses: databricks/run-notebook@v0.0.3
        id: trigger_databricks_notebook_run_scala_etl_job
        with:
          run-name: "PR ${{ github.event.number }} GitHub Action - Run Scala ETL job"
          local-notebook-path: "Run Scala ETL Job.py"
          notebook-params-json:  >
            {
              "env": "dev",
              "repo": "amit-pr-checks-repo",
              "branch": "pr-${{ github.event.number }}-${{ github.event.pull_request.head.sha }}",
              "etl_start_date": "2012-01-01",
              "etl_end_date": "2012-03-01"
            }
          new-cluster-json: >
            {
              "num_workers": 1,
              "spark_version": "14.3.x-scala2.12",
              "node_type_id": "m5d.large",
              "spark_conf": {
                "spark.hadoop.fs.lakefs.access.mode": "presigned",
                "spark.hadoop.fs.lakefs.impl": "io.lakefs.LakeFSFileSystem",
                "spark.hadoop.fs.lakefs.endpoint": "https://treeverse.us-east-1.lakefscloud.io/api/v1",
                "spark.hadoop.fs.lakefs.access.key": "${{secrets.LAKEFS_ACCESS_KEY}}",
                "spark.hadoop.fs.lakefs.secret.key": "${{secrets.LAKEFS_SECRET_KEY}}",
                "spark.hadoop.fs.s3a.access.key": "${{secrets.AWS_ACCESS_KEY}}",
                "spark.hadoop.fs.s3a.secret.key": "${{secrets.AWS_SECRET_KEY}}"
              }
            }
          libraries-json: >
            [
              { "jar": "s3://treeverse-ort-simulation-bucket/amit/jars/pr-${{ github.event.number }}/transform-assembly-0.1.0-SNAPSHOT.jar" },
              { "maven": {"coordinates": "io.lakefs:hadoop-lakefs-assembly:0.2.1"} },
              { "pypi": {"package": "lakefs==0.4.1"} }
            ]
   ```

1. Code to checkout a folder from the repo instead of full repo:

   ```bash
      # Checkout project code
      # Use sparse checkout to only select files in a directory
      # Turning off cone mode ensures that files in the project root are not included during checkout
      - name: Checks out the repo
        uses: actions/checkout@v4
        with:
          sparse-checkout: 'etl_jobs/src'
          sparse-checkout-cone-mode: false
   ```

1. Get branch list and store it in a GitHub multi-line environment variable:

   ```bash
      - name: Get branch list
        run: |
          {
           echo 'PR_BRANCHES<<EOF'
           git log -${{ env.PR_FETCH_DEPTH }} --pretty=format:'%H'
           echo ''
           echo 'EOF'
          } >> $GITHUB_ENV
   ```

1. If you use Scala for Databricks notebooks then this is the step to build Scala job:

   ```bash
   ```

1. If you use Scala for Databricks notebooks then this is the step to build Scala job:

   ```bash
   ```
