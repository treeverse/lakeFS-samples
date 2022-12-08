from airflow.decorators import dag
from airflow.models import Variable
from airflow.utils.dates import days_ago
from lakefs_provider.operators.get_object_operator import LakeFSGetObjectOperator
from lakefs_provider.sensors.file_sensor import LakeFSFileSensor
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

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

def confirm_successful_etl(task_instance, actual: str, expected: str) -> None:
    if actual != expected:
        raise AirflowFailException(f'Got {actual} instead of {expected}')

@dag(default_args=default_args,
     render_template_as_native_obj=True,
     max_active_runs=1,
     start_date=days_ago(2),
     schedule_interval=None,
     tags=['testing'])
def lakefs_hooks_pre_merge_dag():
    # Checks periodically for the path.
    # DAG continues only when the file exists.
    task_sense_success_file = LakeFSFileSensor(
        task_id='sense_success_file',
        branch=default_args.get('default-branch'),
        mode='reschedule',
        poke_interval=2,
        timeout=5,
    )

    # Get the file.
    task_get_success_file = LakeFSGetObjectOperator(
        task_id='get_success_file',
        do_xcom_push=True,
        ref=default_args['default-branch'])

    # Check its contents
    task_confirm_successful_etl = PythonOperator(
        task_id='confirm_successful_etl',
        python_callable=confirm_successful_etl,
        op_kwargs={
            'actual': '''{{ task_instance.xcom_pull(task_ids='get_success_file', key='return_value') }}''',
            'expected': 'Successful',
        })        


    task_sense_success_file >> task_get_success_file >> task_confirm_successful_etl
    
pre_merge_dag = lakefs_hooks_pre_merge_dag()