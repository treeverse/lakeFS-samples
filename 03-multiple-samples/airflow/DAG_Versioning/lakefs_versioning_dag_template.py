from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from lakefs_provider.operators.create_branch_operator import LakeFSCreateBranchOperator
from lakefs_provider.operators.commit_operator import LakeFSCommitOperator
from lakefs_provider.operators.merge_operator import LakeFSMergeOperator
from lakefs_provider.operators.upload_operator import LakeFSUploadOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.models.dagrun import DagRun
import time
from functools import partial
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from io import StringIO
from datetime import datetime
from airflow import DAG


# The execution context and any results are automatically passed by task.post_execute method
def print_commit_result(context, result, message):
    LoggingMixin().log.info(message + result \
        + ' and lakeFS URL is: ' + Variable.get("lakefsUIEndPoint") \
        + '/repositories/' + Variable.get("repo") + '/commits/' + result)

class NamedStringIO(StringIO):
    def __init__(self, content: str, name: str) -> None:
        super().__init__(content)
        self.name = name

def lakefs_versioning_dag_template(dag_id, default_args):

    dag = DAG(dag_id,
             default_args=default_args)

    with dag:    
        jars_partition_data = Variable.get("spark_home") + '/jars/hadoop-aws-*.jar,' + Variable.get("spark_home") + '/jars/aws-java-sdk-bundle-*.jar'
    
        task_transformation = SparkSubmitOperator(
            task_id='transformation',
            conn_id='conn_spark',
            application="./airflow/DAG_Versioning/transformation.py",
            application_args=[default_args.get('branch')],
            jars=jars_partition_data
        )
    
        # The execution context is automatically passed by task.pre_execute method
        task_transformation.pre_execute = lambda context: LoggingMixin().log.info(
            'Branch name is: ' + default_args.get('branch') \
            + ' and lakeFS URL is: ' + Variable.get("lakefsUIEndPoint") \
            + '/repositories/' + Variable.get("repo") + '/objects?ref=' \
            + default_args.get('branch'))
    
    return dag

#branch_name = Variable.get("newBranch", Variable.get("sourceBranch"))

dag_id = 'lakefs_versioning_dag.' + branch_name

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "lakeFS",
    "branch": branch_name,
    "repo": Variable.get("repo"),
    "default-branch": Variable.get("sourceBranch"),
    "lakefs_conn_id": Variable.get("conn_lakefs"),
     "render_template_as_native_obj":True,
     "max_active_runs":1,
     "start_date":days_ago(0),
     "schedule_interval":None,
     "catchup":False,
     "tags":['testing']
}

globals()[dag_id] = lakefs_versioning_dag_template(dag_id, default_args)