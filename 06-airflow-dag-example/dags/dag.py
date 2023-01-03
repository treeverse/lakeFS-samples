from datetime import datetime, timedelta

import extract
import transform
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from dulwich.repo import Repo
from lakefs_client.model.reset_creation import ResetCreation
from lakefs_provider.hooks.lakefs_hook import LakeFSHook
from lakefs_provider.operators.commit_operator import LakeFSCommitOperator
from lakefs_provider.operators.create_branch_operator import \
    LakeFSCreateBranchOperator
from lakefs_provider.operators.merge_operator import LakeFSMergeOperator

from airflow import DAG

RAW_ROW_COUNT = 100000

try:
    # This can be any code that fetches the current DAG version
    r = Repo(Variable.get("dag_code_git_root"))
    git_sha = r.head().decode("utf-8")
except Exception as e:
    print(f"Failed to get git sha: {e}")
    git_sha = "unknown"


def get_version():
    return git_sha


def _transform_steps(dt_arg, transform_name, transform_func):
    branch = f"{transform_name}_{dt_arg}"

    def _reset_branch(context):
        ti = context["ti"]
        dt = ti.xcom_pull(task_ids='extract', dag_id='etl', key='return_value')
        branch = f"{transform_name}_{dt}"
        lakefs_hook = LakeFSHook(lakefs_conn_id="lakefs")
        print("XXXDX" + branch)
        lakefs_hook.get_conn().branches.reset_branch(repository="example-repo", branch=branch,
                                                     reset_creation=ResetCreation(type="reset"))

    create_branch_op = LakeFSCreateBranchOperator(
        task_id=f"{transform_name}_create_branch",
        repo="example-repo",
        branch=branch,
        lakefs_conn_id="lakefs",
        source_branch="main",
    )

    transform_op = PythonOperator(task_id=f"{transform_name}_transform",
                                          python_callable=transform_func,
                                          op_kwargs={"dt": dt_arg},
                                          retries=5,
                                          retry_delay=timedelta(seconds=5),
                                          on_retry_callback=_reset_branch)

    commit_op = LakeFSCommitOperator(
        task_id=f"{transform_name}_commit",
        repo="example-repo",
        branch=branch,
        lakefs_conn_id="lakefs",
        msg="Transform result",
        metadata={
            "dag_version": get_version(),
            "transform_version": transform.get_version(),
            "extract_version": extract.get_version(),
        },
    )

    merge_op = LakeFSMergeOperator(
        task_id=f"{transform_name}_merge",
        repo="example-repo",
        lakefs_conn_id="lakefs",
        source_ref=branch,
        destination_branch="main",
        msg="Merge transform result",
        metadata={
            "dag_version": get_version(),
            "transform_version": transform.get_version(),
            "extract_version": extract.get_version(),
        },
    )
    return (merge_op << commit_op << transform_op << create_branch_op)


with DAG(
    dag_id="etl",
    catchup=False,
    schedule="@daily",
    start_date=datetime(2021, 1, 1),
    tags=["produces", "dataset-scheduled"],
) as dag1:
    t1 = PythonOperator(task_id="extract", python_callable=extract.extract)
    t2 = LakeFSCommitOperator(
        task_id="commit",
        repo="example-repo",
        branch="main",
        lakefs_conn_id="lakefs",
        msg="Extract result",
        metadata={
            "dag_version": get_version(),
            "transform_version": transform.get_version(),
            "extract_version": extract.get_version(),
        },
    )
    t3 = _transform_steps(t1.output, "total_by_user",
                          transform.transform_total_by_user)
    t4 = _transform_steps(t1.output, "total_by_event_type",
                          transform.transform_total_by_event_type)
    t5 = _transform_steps(t1.output, "total_by_month",
                          transform.transform_total_by_month)
    t1 >> t2 >> [t3, t4, t5]
