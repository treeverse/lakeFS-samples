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

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "lakeFS",
    "branch": Variable.get("newBranch") + '_{{ ts_nodash }}',
    "repo": Variable.get("repo"),
    "path": Variable.get("successFileName"),
    "default-branch": Variable.get("sourceBranch"),
    "lakefs_conn_id": Variable.get("conn_lakefs")
}

# The execution context and any results are automatically passed by task.post_execute method
def print_commit_result(context, result, message):
    LoggingMixin().log.info(message + result \
        + ' and lakeFS URL is: ' + Variable.get("lakefsUIEndPoint") \
        + '/repositories/' + Variable.get("repo") + '/commits/' + result)

class NamedStringIO(StringIO):
    def __init__(self, content: str, name: str) -> None:
        super().__init__(content)
        self.name = name
    
@dag(default_args=default_args,
     render_template_as_native_obj=True,
     max_active_runs=1,
     start_date=days_ago(2),
     schedule_interval=None,
     tags=['testing'])
def lakefs_hooks_post_commit_dag():
    # Create the branch to run on
    task_create_etl_branch = LakeFSCreateBranchOperator(
        task_id='create_etl_branch',
        branch=default_args.get('branch'),
        source_branch=default_args.get('default-branch')
    )
    
    task_create_etl_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    jars_partition_data = Variable.get("spark_home") + '/jars/hadoop-aws-*.jar,' + Variable.get("spark_home") + '/jars/aws-java-sdk-bundle-*.jar'
    
    task_transformation = SparkSubmitOperator(
        task_id='transformation',
        conn_id='conn_spark',
        application="./airflow/Hooks/transformation.py",
        application_args=[default_args.get('branch')],
        jars=jars_partition_data
    )
    
    # The execution context is automatically passed by task.pre_execute method
    task_transformation.pre_execute = lambda context: LoggingMixin().log.info(
        'Branch name is: ' + Variable.get("newBranch") + '_' \
        + context['ts_nodash'] \
        + ' and lakeFS URL is: ' + Variable.get("lakefsUIEndPoint") \
        + '/repositories/' + Variable.get("repo") + '/objects?ref=' \
        + Variable.get("newBranch") + '_' + context['ts_nodash'] )

    task_create_success_file = LakeFSUploadOperator(
        task_id='create_success_file',
        content=NamedStringIO(content='Successful', name='content'))

    task_create_success_file.post_execute = lambda context, result: LoggingMixin().log.info(
        'lakeFS URL for the data file is: ' + Variable.get("lakefsUIEndPoint") \
        + '/api/v1/repositories/' + Variable.get("repo") + '/refs/' \
        + Variable.get("newBranch") + '_' + context['ts_nodash'] \
        + '/objects?path=' + Variable.get("successFileName"))
    
    task_commit_etl_branch = LakeFSCommitOperator(
        task_id='commit_etl_branch',
        branch=default_args.get('branch'),
        msg='committing to lakeFS using airflow!',
        metadata={"committed_from": "airflow-operator"}
    )

    task_commit_etl_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    # Merge the changes back to the main branch.
    task_merge_etl_to_ingest_branch = LakeFSMergeOperator(
        task_id='merge_etl_to_ingest_branch',
        do_xcom_push=True,
        source_ref=default_args.get('branch'),
        destination_branch=default_args.get('default-branch'),
        msg='merging ' + default_args.get('branch') + ' to the ' + default_args.get('default-branch') + ' branch',
        metadata={"committer": "airflow-operator"}
    )

    task_merge_etl_to_ingest_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    task_create_etl_branch >> task_transformation >> task_create_success_file >> task_commit_etl_branch >> task_merge_etl_to_ingest_branch
    
post_commit_dag = lakefs_hooks_post_commit_dag()