from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable

from lakefs_provider.operators.get_object_operator import LakeFSGetObjectOperator

import fileinput

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "lakeFS",
    "branch": Variable.get("newBranch"),
    "repo": Variable.get("repo"),
    "path": Variable.get("fileName"),
    "default-branch": Variable.get("sourceBranch"),
    "lakefs_conn_id": Variable.get("conn_lakefs")
}

with DAG(
    dag_id='lakefs_create_dag',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    catchup=False,
    tags=['testing'],
) as dag:
    # Get the file.
    task_get_dag_template = LakeFSGetObjectOperator(
        task_id='get_dag_template',
        do_xcom_push=True,
        ref=Variable.get("sourceBranch"),
        path=Variable.get("dags_folder_on_lakefs")+'/'+Variable.get("dag_template_filename")
        )

    @task(task_id="create_dag")
    def create_dag_file(dag_template, **kwargs):
        branch_name = kwargs['params']['lakeFS_event']['branch_id']
        dag_id = Variable.get("dag_name") + '.' + branch_name

        new_filename = kwargs['conf'].get('core', 'dags_folder') + '/' + dag_id + '.py'
        with open(new_filename, 'w') as f:
            f.write(dag_template)

        with fileinput.input(new_filename, inplace=True) as file:
            for line in file:
                new_line = line.replace('branch_name', "'" + branch_name + "'")
                print(new_line, end='')

    dag_template = '''{{ task_instance.xcom_pull(task_ids='get_dag_template', key='return_value') }}'''
    task_create_dag = create_dag_file(dag_template)
    
    task_get_dag_template >> task_create_dag