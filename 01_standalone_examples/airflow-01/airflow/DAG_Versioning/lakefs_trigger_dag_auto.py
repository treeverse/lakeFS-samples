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
    dag_id='lakefs_trigger_dag',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    catchup=False,
    tags=['testing'],
) as dag:
    task_unpause_dag = BashOperator(
        task_id='unpause_dag',
        bash_command='airflow dags unpause ' + Variable.get("dag_name") + '.{{ params["lakeFS_event"]["branch_id"] }}'
    )
    
    task_trigger_dag = BashOperator(
        task_id='trigger_dag',
        bash_command='airflow dags trigger ' + Variable.get("dag_name") + '.{{ params["lakeFS_event"]["branch_id"] }}'
    )
        
    task_unpause_dag >> task_trigger_dag