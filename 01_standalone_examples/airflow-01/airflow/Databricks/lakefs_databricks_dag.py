from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.providers.databricks.operators.databricks import (
    DatabricksCreateJobsOperator,
    DatabricksRunNowOperator,
)

default_args = {
    "owner": "lakeFS",
}

with DAG('lakefs_databricks_dag',
    default_args=default_args,
    render_template_as_native_obj=True,
    max_active_runs=1,
    start_date=days_ago(2),
    schedule=None,
    tags=['testing']
  ) as dag:

    job = {
        "name": Variable.get("databricksJobName"),
        "tasks": [
            {
                "task_key": Variable.get("databricksTaskName"),
                "job_cluster_key": Variable.get("databricksClusterName"),
                "notebook_task": {
                    "notebook_path": Variable.get("databricksNotebookPath"),
                },
                "libraries": [
                    {
                        "pypi": {
                            "package": "lakefs"
                        },
                    },
                    {
                        "maven": {
                            "coordinates": "io.lakefs:hadoop-lakefs-assembly:0.2.1"
                        },
                    }
                ],
            },
        ],
        "job_clusters": [
            {
                "job_cluster_key": Variable.get("databricksClusterName"),
                "new_cluster": {
                    "spark_version": Variable.get("databricksSparkVersion"),
                    "node_type_id": Variable.get("databricksNodeType"),
                    "num_workers": Variable.get("databricksNumberOfWorkers"),
                    "spark_conf": {
                        "spark.hadoop.fs.lakefs.access.mode": "presigned",
                        "spark.hadoop.fs.lakefs.impl": "io.lakefs.LakeFSFileSystem",
                        "spark.hadoop.fs.lakefs.endpoint": Variable.get("lakefsEndPoint")+'/api/v1',
                        "spark.hadoop.fs.lakefs.access.key": Variable.get("lakefsAccessKey"),
                        "spark.hadoop.fs.lakefs.secret.key": Variable.get("lakefsSecretKey"),
                    },
                },
            },
        ],
    }
    
    create_databricks_job = DatabricksCreateJobsOperator(
        task_id="create_databricks_job",
        databricks_conn_id = Variable.get("conn_databricks"),
        json=job)

    run_databricks_job_now = DatabricksRunNowOperator(
        task_id="run_databricks_job_now",
        databricks_conn_id = Variable.get("conn_databricks"),
        job_id="{{ ti.xcom_pull(task_ids='create_databricks_job') }}"
    )

    create_databricks_job >> run_databricks_job_now