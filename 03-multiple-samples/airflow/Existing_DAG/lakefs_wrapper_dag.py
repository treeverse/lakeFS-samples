from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from lakefs_provider.operators.create_branch_operator import LakeFSCreateBranchOperator
from lakefs_provider.operators.commit_operator import LakeFSCommitOperator
from lakefs_provider.operators.merge_operator import LakeFSMergeOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.models.dagrun import DagRun
import time
from functools import partial
from airflow.utils.log.logging_mixin import LoggingMixin

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "lakeFS",
    "branch": Variable.get("newBranch") + '_{{ ts_nodash }}',
    "repo": Variable.get("repo"),
    #"path": Variable.get("fileName"),
    "default-branch": Variable.get("sourceBranch"),
    "lakefs_conn_id": Variable.get("conn_lakefs")
}

# The execution context and any results are automatically passed by task.post_execute method
def print_commit_result(context, result, message):
    LoggingMixin().log.info(message + result \
        + ' and lakeFS URL is: ' + Variable.get("lakefsUIEndPoint") \
        + '/repositories/' + Variable.get("repo") + '/commits/' + result)

    
@dag(default_args=default_args,
     render_template_as_native_obj=True,
     max_active_runs=1,
     start_date=days_ago(2),
     schedule_interval=None,
     tags=['testing'])
def lakefs_wrapper_dag():
    # Create the branch to run on
    task_create_etl_branch = LakeFSCreateBranchOperator(
        task_id='create_etl_branch',
        branch=default_args.get('branch'),
        source_branch=default_args.get('default-branch')
    )
    
    task_create_etl_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    task_trigger = TriggerDagRunOperator(
        task_id="trigger_existing_dag",
        trigger_dag_id="lakefs_tutorial_taskflow_api_etl",  # Ensure this equals the dag_id of the DAG to trigger
        wait_for_completion="True",
        poke_interval=5,
        conf={ 'newBranch': default_args.get('branch') }
    )

    # The execution context is automatically passed by task.pre_execute method
    task_trigger.pre_execute = lambda context: LoggingMixin().log.info(
        'Branch name is: ' + Variable.get("newBranch") + '_' \
        + context['ts_nodash'] \
        + ' and lakeFS URL is: ' + Variable.get("lakefsUIEndPoint") \
        + '/repositories/' + Variable.get("repo") + '/objects?ref=' \
        + Variable.get("newBranch") + '_' + context['ts_nodash'] )

    task_commit_etl_branch = LakeFSCommitOperator(
        task_id='commit_etl_branch',
        branch=default_args.get('branch'),
        msg='committing to lakeFS using airflow!',
        metadata={"committed_from": "airflow-operator"}
    )

    task_commit_etl_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    # Merge the changes back to the main branch.
    task_merge_etl_branch = LakeFSMergeOperator(
        task_id='merge_etl_branch',
        do_xcom_push=True,
        source_ref=default_args.get('branch'),
        destination_branch=default_args.get('default-branch'),
        msg='merging ' + default_args.get('branch') + ' to the ' + default_args.get('default-branch') + ' branch',
        metadata={"committer": "airflow-operator"}
    )

    task_merge_etl_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    task_create_etl_branch >> task_trigger >> task_commit_etl_branch >> task_merge_etl_branch
    
sample_workflow_dag = lakefs_wrapper_dag()