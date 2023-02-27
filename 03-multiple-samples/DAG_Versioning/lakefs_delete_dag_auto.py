from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "lakeFS"
}

with DAG(
    dag_id='lakefs_delete_dag',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    catchup=False,
    tags=['testing'],
) as dag:
    task_delete_dag_file = BashOperator(
        task_id='delete_dag_file',
        bash_command='rm $HOME/airflow/dags/' + Variable.get("dag_name") + '.{{ params["lakeFS_event"]["branch_id"] }}.py'
    )
    
    task_delete_dag = BashOperator(
        task_id='delete_dag',
        bash_command='airflow dags delete ' + Variable.get("dag_name") + '.{{ params["lakeFS_event"]["branch_id"] }} --yes'
    )
    
    task_delete_dag_file >> task_delete_dag