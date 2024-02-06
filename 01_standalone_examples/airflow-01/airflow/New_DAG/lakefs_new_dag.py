from typing import Dict
from collections.abc import Sequence

from collections import namedtuple
from itertools import zip_longest

from io import StringIO

from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException

from lakefs_provider.hooks.lakefs_hook import LakeFSHook
from lakefs_provider.operators.create_branch_operator import LakeFSCreateBranchOperator
from lakefs_provider.operators.merge_operator import LakeFSMergeOperator
from lakefs_provider.operators.upload_operator import LakeFSUploadOperator
from lakefs_provider.operators.commit_operator import LakeFSCommitOperator
from lakefs_provider.operators.get_commit_operator import LakeFSGetCommitOperator
from lakefs_provider.operators.get_object_operator import LakeFSGetObjectOperator
from lakefs_provider.sensors.file_sensor import LakeFSFileSensor
from lakefs_provider.sensors.commit_sensor import LakeFSCommitSensor
from airflow.operators.python import PythonOperator

from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from functools import partial

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "lakeFS",
    "branch": Variable.get("newBranch") + '_{{ ts_nodash }}',
    "repo": Variable.get("repo"),
    "path": Variable.get("fileName"),
    "default-branch": Variable.get("sourceBranch"),
    "lakefs_conn_id": Variable.get("conn_lakefs")
}

filePath = Variable.get("filePath")
fileName = Variable.get("fileName")
CONTENT = open(filePath+'/'+fileName, 'rb').read()

IdAndMessage = namedtuple('IdAndMessage', ['id', 'message'])


def check_equality(task_instance, actual: str, expected: str) -> None:
    if actual != expected:
        raise AirflowFailException(f'Got {actual} instead of {expected}')


def check_logs(task_instance, repo: str, ref: str, commits: Sequence[str], messages: Sequence[str], amount: int=100) -> None:
    hook = LakeFSHook(default_args['lakefs_conn_id'])
    expected = [ IdAndMessage(commit, message) for commit, message in zip(commits, messages) ]
    # Print commit logs for debugging purpose
    #actuals = (IdAndMessage(message=commit['message'], id=commit['id'])
    #           for commit in hook.log_commits(repo, ref, amount))
    #for (actual) in zip_longest(actuals):
    #    print(actual)
    actuals = (IdAndMessage(message=commit['message'], id=commit['id'])
               for commit in hook.log_commits(repo, ref, amount))
    for (expected, actual) in zip_longest(expected, actuals):
        if expected is None:
            # Matched all msgs!
            return
        if expected != actual:
            raise AirflowFailException(f'Got {actual} instead of {expected}')

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
def lakefs_new_dag():
    """
    ### Example lakeFS DAG

    Showcases the lakeFS provider package's operators and sensors.

    To run this example, create a connector with:
    - id: conn_lakefs
    - type: http
    - host: http://localhost:8000
    - extra: {"access_key_id":"AKIAIOSFOLKFSSAMPLES","secret_access_key":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}
    """

    # Create the branch to run on
    task_create_etl_branch = LakeFSCreateBranchOperator(
        task_id='create_etl_branch',
        branch=default_args.get('branch'),
        source_branch=default_args.get('default-branch')
    )
    
    task_create_etl_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    task_create_etl_load_branch = LakeFSCreateBranchOperator(
        task_id='create_etl_load_branch',
        branch=default_args.get('branch') + '_etl_load',
        source_branch=default_args.get('branch')
    )

    task_create_etl_load_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    task_create_etl_task1_branch = LakeFSCreateBranchOperator(
        task_id='create_etl_task1_branch',
        branch=default_args.get('branch') + '_etl_task1',
        source_branch=default_args.get('branch') + '_etl_load'
    )

    task_create_etl_task1_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    task_create_etl_task2_branch = LakeFSCreateBranchOperator(
        task_id='create_etl_task2_branch',
        branch=default_args.get('branch') + '_etl_task2',
        source_branch=default_args.get('branch') + '_etl_load'
    )

    task_create_etl_task2_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    task_create_etl_task3_branch = LakeFSCreateBranchOperator(
        task_id='create_etl_task3_branch',
        branch=default_args.get('branch') + '_etl_task3',
        source_branch=default_args.get('branch') + '_etl_load'
    )

    task_create_etl_task3_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    # Create a path.
    task_load_file = LakeFSUploadOperator(
        task_id='load_file',
        branch=default_args.get('branch') + '_etl_load',
        content=CONTENT)

    task_load_file.post_execute = lambda context, result: LoggingMixin().log.info(
        'lakeFS URL for the data file is: ' + Variable.get("lakefsUIEndPoint") \
        + '/api/v1/repositories/' + Variable.get("repo") + '/refs/' \
        + Variable.get("newBranch") + '_' + context['ts_nodash'] \
        + '_etl_load' + '/objects?path=' + Variable.get("fileName"))

    task_get_etl_branch_commit = LakeFSGetCommitOperator(
        do_xcom_push=True,
        task_id='get_etl_branch_commit',
        ref=default_args['branch'])

    # Checks periodically for the path.
    # DAG continues only when the file exists.
    task_sense_file = LakeFSFileSensor(
        task_id='sense_file',
        branch=default_args.get('branch') + '_etl_load',
        mode='reschedule',
        poke_interval=1,
        timeout=20,
    )

    # Commit the changes to the branch.
    # (Also a good place to validate the new changes before committing them)
    task_commit_load = LakeFSCommitOperator(
        task_id='commit_load',
        branch=default_args.get('branch') + '_etl_load',
        msg='committing etl_load to lakeFS using airflow!',
        metadata={"committed_from": "airflow-operator"}
    )

    # The execution context and any results are automatically passed by task.post_execute method
    task_commit_load.post_execute = partial(print_commit_result, message='lakeFS commit id for load_file task is: ')
    
    task_commit_task1 = LakeFSCommitOperator(
        task_id='commit_task1',
        branch=default_args.get('branch') + '_etl_task1',
        msg='committing etl_task1 to lakeFS using airflow!',
        metadata={"committed_from": "airflow-operator"}
    )

    # The execution context and any results are automatically passed by task.post_execute method
    task_commit_task1.post_execute = partial(print_commit_result, message='lakeFS commit id for etl_task1 is: ')
    
    task_commit_task2_1 = LakeFSCommitOperator(
        task_id='commit_task2_1',
        branch=default_args.get('branch') + '_etl_task2',
        msg='committing etl_task2_1 to lakeFS using airflow!',
        metadata={"committed_from": "airflow-operator"}
    )

    # The execution context and any results are automatically passed by task.post_execute method
    task_commit_task2_1.post_execute = partial(print_commit_result, message='lakeFS commit id for etl_task2_1 is: ')
    
    task_commit_task2_2 = LakeFSCommitOperator(
        task_id='commit_task2_2',
        branch=default_args.get('branch') + '_etl_task2',
        msg='committing etl_task2_2 to lakeFS using airflow!',
        metadata={"committed_from": "airflow-operator"}
    )

    # The execution context and any results are automatically passed by task.post_execute method
    task_commit_task2_2.post_execute = partial(print_commit_result, message='lakeFS commit id for etl_task2_2 is: ')
    
    task_commit_task2_3 = LakeFSCommitOperator(
        task_id='commit_task2_3',
        branch=default_args.get('branch') + '_etl_task2',
        msg='committing etl_task2_3 to lakeFS using airflow!',
        metadata={"committed_from": "airflow-operator"}
    )

    # The execution context and any results are automatically passed by task.post_execute method
    task_commit_task2_3.post_execute = partial(print_commit_result, message='lakeFS commit id for etl_task2_3 is: ')
    
    task_commit_task3 = LakeFSCommitOperator(
        task_id='commit_task3',
        branch=default_args.get('branch') + '_etl_task3',
        msg='committing etl_task3 to lakeFS using airflow!',
        metadata={"committed_from": "airflow-operator"}
    )

    # The execution context and any results are automatically passed by task.post_execute method
    task_commit_task3.post_execute = partial(print_commit_result, message='lakeFS commit id for etl_task3 is: ')
    
    # Wait until the commit is completed.
    # Not really necessary in this DAG, since the LakeFSCommitOperator won't return before that.
    # Nonetheless we added it to show the full capabilities.
    task_sense_commit_etl = LakeFSCommitSensor(
        task_id='sense_commit_etl',
        prev_commit_id='''{{ task_instance.xcom_pull(task_ids='get_etl_branch_commit', key='return_value').id }}''',
        mode='reschedule',
        poke_interval=1,
        timeout=360,
    )

    # Get the file.
    task_get_file = LakeFSGetObjectOperator(
        task_id='get_file',
        do_xcom_push=True,
        ref=default_args['branch'] + '_etl_load')

    # Check its contents
    task_check_equality = PythonOperator(
        task_id='check_equality',
        python_callable=check_equality,
        op_kwargs={
            'actual': '''{{ task_instance.xcom_pull(task_ids='get_file', key='return_value') }}''',
            'expected': str(CONTENT, 'utf-8'),
        })        

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

    task_merge_etl_task1_branch = LakeFSMergeOperator(
        task_id='merge_etl_task1_branch',
        do_xcom_push=True,
        source_ref=default_args.get('branch')+'_etl_task1',
        destination_branch=default_args.get('branch')+'_etl_load',
        msg='merging ' + default_args.get('branch')+'_etl_task1' + ' to the ' + default_args.get('branch')+'_etl_load' + ' branch',
        metadata={"committer": "airflow-operator"}
    )

    task_merge_etl_task1_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    task_merge_etl_task2_branch = LakeFSMergeOperator(
        task_id='merge_etl_task2_branch',
        do_xcom_push=True,
        source_ref=default_args.get('branch')+'_etl_task2',
        destination_branch=default_args.get('branch')+'_etl_load',
        msg='merging ' + default_args.get('branch')+'_etl_task2' + ' to the ' + default_args.get('branch')+'_etl_load' + ' branch',
        metadata={"committer": "airflow-operator"}
    )

    task_merge_etl_task2_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    task_merge_etl_task3_branch = LakeFSMergeOperator(
        task_id='merge_etl_task3_branch',
        do_xcom_push=True,
        source_ref=default_args.get('branch') + '_etl_task3',
        destination_branch=default_args.get('branch') + '_etl_load',
        msg='merging ' + default_args.get('branch') + '_etl_task3' + ' to the ' + default_args.get('branch') + '_etl_load' + ' branch',
        metadata={"committer": "airflow-operator"}
    )

    task_merge_etl_task3_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    task_merge_etl_load_branch = LakeFSMergeOperator(
        task_id='merge_etl_load_branch',
        #do_xcom_push=True,
        source_ref=default_args.get('branch')+'_etl_load',
        destination_branch=default_args.get('branch'),
        msg='merging ' + default_args.get('branch') + '_etl_load' + ' to the ' + default_args.get('branch') + ' branch',
        metadata={"committer": "airflow-operator"}
    )

    task_merge_etl_load_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    expectedCommits = ['''{{ ti.xcom_pull('merge_etl_branch') }}''']
    expectedMessages = ['merging ' + default_args.get('branch') + ' to the ' + default_args.get('default-branch') + ' branch']

    # Fetch and verify log messages in bulk.
    task_check_logs_bulk = PythonOperator(
        task_id='check_logs_bulk',
        python_callable=check_logs,
        op_kwargs={
            'repo': default_args.get('repo'),
            'ref': '''{{ task_instance.xcom_pull(task_ids='merge_etl_branch', key='return_value') }}''',
            'commits': expectedCommits,
            'messages': expectedMessages,
        })

    # Fetch and verify log messages one at a time.
    task_check_logs_individually = PythonOperator(
        task_id='check_logs_individually',
        python_callable=check_logs,
        op_kwargs= {
            'repo': default_args.get('repo'),
            'ref': '''{{ task_instance.xcom_pull(task_ids='merge_etl_branch', key='return_value') }}''',
            'amount': 1,
            'commits': expectedCommits,
            'messages': expectedMessages,
        })

    expectedEtlTask1Commits = ['''{{ ti.xcom_pull('commit_task1') }}''',
                               '''{{ ti.xcom_pull('commit_load') }}''']
    expectedEtlTask1Messages = ['committing etl_task1 to lakeFS using airflow!',
                                'committing etl_load to lakeFS using airflow!']
    
    # Fetch and verify log messages in bulk.
    task_check_etl_task1_branch_logs_bulk = PythonOperator(
        task_id='check_etl_task1_branch_logs_bulk',
        python_callable=check_logs,
        op_kwargs={
            'repo': default_args.get('repo'),
            'ref': '''{{ task_instance.xcom_pull(task_ids='commit_task1', key='return_value') }}''',
            'commits': expectedEtlTask1Commits,
            'messages': expectedEtlTask1Messages,
        })

    expectedEtlTask2Commits = ['''{{ ti.xcom_pull('commit_task2_3') }}''',
                               '''{{ ti.xcom_pull('commit_task2_2') }}''',
                               '''{{ ti.xcom_pull('commit_task2_1') }}''',
                               '''{{ ti.xcom_pull('commit_load') }}''']
    expectedEtlTask2Messages = ['committing etl_task2_3 to lakeFS using airflow!',
                                'committing etl_task2_2 to lakeFS using airflow!',
                                'committing etl_task2_1 to lakeFS using airflow!',
                                'committing etl_load to lakeFS using airflow!']
    
    # Fetch and verify log messages in bulk.
    task_check_etl_task2_branch_logs_bulk = PythonOperator(
        task_id='check_etl_task2_branch_logs_bulk',
        python_callable=check_logs,
        op_kwargs={
            'repo': default_args.get('repo'),
            'ref': '''{{ task_instance.xcom_pull(task_ids='commit_task2_3', key='return_value') }}''',
            'commits': expectedEtlTask2Commits,
            'messages': expectedEtlTask2Messages,
        })

    expectedEtlTask3Commits = ['''{{ ti.xcom_pull('commit_task3') }}''',
                               '''{{ ti.xcom_pull('commit_load') }}''']
    expectedEtlTask3Messages = ['committing etl_task3 to lakeFS using airflow!',
                                'committing etl_load to lakeFS using airflow!']
    
    # Fetch and verify log messages in bulk.
    task_check_etl_task3_branch_logs_bulk = PythonOperator(
        task_id='check_etl_task3_branch_logs_bulk',
        python_callable=check_logs,
        op_kwargs={
            'repo': default_args.get('repo'),
            'ref': '''{{ task_instance.xcom_pull(task_ids='commit_task3', key='return_value') }}''',
            'commits': expectedEtlTask3Commits,
            'messages': expectedEtlTask3Messages,
        })

    jars_partition_data = Variable.get("spark_home") + '/jars/hadoop-aws-*.jar,' + Variable.get("spark_home") + '/jars/aws-java-sdk-bundle-*.jar'
    
    task_etl_task1 = SparkSubmitOperator(
        task_id='etl_task1',
        conn_id='conn_spark',
        application="/home/jovyan/airflow/New_DAG/etl_task1.py",
        application_args=[default_args.get('branch') + '_etl_task1'],
        jars=jars_partition_data
    )
    
    # The execution context is automatically passed by task.pre_execute method
    task_etl_task1.pre_execute = lambda context: LoggingMixin().log.info(
        'Branch name is: ' + Variable.get("newBranch") + '_' \
        + context['ts_nodash'] + '_etl_task1' \
        + ' and lakeFS URL is: ' + Variable.get("lakefsUIEndPoint") \
        + '/repositories/' + Variable.get("repo") + '/objects?ref=' \
        + Variable.get("newBranch") + '_' + context['ts_nodash'] + '_etl_task1' )

    task_etl_task2_1 = SparkSubmitOperator(
        task_id='etl_task2_1',
        conn_id='conn_spark',
        application="/home/jovyan/airflow/New_DAG/etl_task2_1.py",
        application_args=[default_args.get('branch') + '_etl_task2'],
        jars=jars_partition_data
    )
    
    # The execution context is automatically passed by task.pre_execute method
    task_etl_task2_1.pre_execute = lambda context: LoggingMixin().log.info(
        'Branch name is: ' + Variable.get("newBranch") + '_' \
        + context['ts_nodash'] + '_etl_task2' \
        + ' and lakeFS URL is: ' + Variable.get("lakefsUIEndPoint") \
        + '/repositories/' + Variable.get("repo") + '/objects?ref=' \
        + Variable.get("newBranch") + '_' + context['ts_nodash'] + '_etl_task2' )

    task_etl_task2_2 = SparkSubmitOperator(
        task_id='etl_task2_2',
        conn_id='conn_spark',
        application="/home/jovyan/airflow/New_DAG/etl_task2_2.py",
        application_args=[default_args.get('branch') + '_etl_task2'],
        jars=jars_partition_data
    )
    
    # The execution context is automatically passed by task.pre_execute method
    task_etl_task2_2.pre_execute = lambda context: LoggingMixin().log.info(
        'Branch name is: ' + Variable.get("newBranch") + '_' \
        + context['ts_nodash'] + '_etl_task2' \
        + ' and lakeFS URL is: ' + Variable.get("lakefsUIEndPoint") \
        + '/repositories/' + Variable.get("repo") + '/objects?ref=' \
        + Variable.get("newBranch") + '_' + context['ts_nodash'] + '_etl_task2' )

    task_etl_task2_3 = SparkSubmitOperator(
        task_id='etl_task2_3',
        conn_id='conn_spark',
        application="/home/jovyan/airflow/New_DAG/etl_task2_3.py",
        application_args=[default_args.get('branch') + '_etl_task2'],
        jars=jars_partition_data
    )
    
    # The execution context is automatically passed by task.pre_execute method
    task_etl_task2_3.pre_execute = lambda context: LoggingMixin().log.info(
        'Branch name is: ' + Variable.get("newBranch") + '_' \
        + context['ts_nodash'] + '_etl_task2' \
        + ' and lakeFS URL is: ' + Variable.get("lakefsUIEndPoint") \
        + '/repositories/' + Variable.get("repo") + '/objects?ref=' \
        + Variable.get("newBranch") + '_' + context['ts_nodash'] + '_etl_task2' )

    task_etl_task3 = SparkSubmitOperator(
        task_id='etl_task3',
        conn_id='conn_spark',
        application="/home/jovyan/airflow/New_DAG/etl_task3.py",
        application_args=[default_args.get('branch') + '_etl_task3'],
        jars=jars_partition_data
    )
    
    # The execution context is automatically passed by task.pre_execute method
    task_etl_task3.pre_execute = lambda context: LoggingMixin().log.info(
        'Branch name is: ' + Variable.get("newBranch") + '_' \
        + context['ts_nodash'] + '_etl_task3' \
        + ' and lakeFS URL is: ' + Variable.get("lakefsUIEndPoint") \
        + '/repositories/' + Variable.get("repo") + '/objects?ref=' \
        + Variable.get("newBranch") + '_' + context['ts_nodash'] + '_etl_task3' )

    task_create_etl_branch >> task_get_etl_branch_commit \
        >> [task_create_etl_load_branch, task_sense_file, task_sense_commit_etl]
    
    task_create_etl_load_branch >> task_load_file >> task_commit_load \
        >> [task_create_etl_task1_branch, task_create_etl_task2_branch, task_create_etl_task3_branch]
    
    task_create_etl_task1_branch >> task_etl_task1 >> task_commit_task1 \
        >> task_check_etl_task1_branch_logs_bulk >> task_merge_etl_task1_branch >> task_merge_etl_load_branch
    
    task_create_etl_task2_branch >> task_etl_task2_1 >> task_commit_task2_1 \
        >> task_etl_task2_2 >> task_commit_task2_2 >> task_etl_task2_3 \
        >> task_commit_task2_3 >> task_check_etl_task2_branch_logs_bulk \
        >> task_merge_etl_task2_branch >> task_merge_etl_load_branch
    
    task_create_etl_task3_branch >> task_etl_task3 >> task_commit_task3 \
        >> task_check_etl_task3_branch_logs_bulk >> task_merge_etl_task3_branch \
        >> task_merge_etl_load_branch
    
    task_sense_file >> task_get_file >> task_check_equality
    
    task_sense_commit_etl >> task_merge_etl_branch \
        >> [task_check_logs_bulk, task_check_logs_individually]

sample_workflow_dag = lakefs_new_dag()