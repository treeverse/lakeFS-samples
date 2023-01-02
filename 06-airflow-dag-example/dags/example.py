from datetime import datetime, timedelta
from io import BytesIO

import boto3
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from lakefs_provider.operators.commit_operator import LakeFSCommitOperator
from lakefs_provider.operators.create_branch_operator import LakeFSCreateBranchOperator
from lakefs_provider.operators.merge_operator import LakeFSMergeOperator

RAW_ROW_COUNT = 100000

s3 = boto3.client('s3',
                  endpoint_url='http://lakefs:8000/',
                  aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
                  aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
                  )


def _random_dates():
    deltas = np.random.randint(0, 60 * 60 * 24 * 365, RAW_ROW_COUNT)
    base = datetime.now() - timedelta(days=365)
    return [datetime.fromtimestamp(base.timestamp() + delta).isoformat() for delta in deltas]


def _save_data_frame(table_name, dt, df, branch="main"):
    csv = df.to_csv().encode('utf-8')
    s3.put_object(
        Bucket="example-repo",
        Key=f"{branch}/{table_name}/{dt}/values.csv",
        Body=BytesIO(csv),
        ContentLength=len(csv),
        ContentType='application/csv'
    )


# Extracts raw data and uploads it to lakeFS
def extract():
    np.random.seed(datetime.now().microsecond)  # TODO - remove this usage of seed
    df = pd.DataFrame({
        'user_id': np.random.randint(1, 1000, RAW_ROW_COUNT),
        'event_value': np.random.rand(RAW_ROW_COUNT),
        'event_type': np.random.choice(['click', 'purchase', 'view'], RAW_ROW_COUNT),
        'event_time': pd.array(_random_dates(), dtype="string"),
    })
    dt = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    _save_data_frame("raw_data", dt, df)
    return dt


def _transform_steps(dt_arg, transform_name, transform_func):
    create_branch_op = LakeFSCreateBranchOperator(
        task_id=f"{transform_name}_create_branch",
        repo="example-repo",
        branch=f"{transform_name}_{dt_arg}",
        lakefs_conn_id="lakefs",
        source_branch="main",
    )

    transform_op = PythonOperator(task_id=f"{transform_name}_transform",
                                          python_callable=transform_func,
                                          op_kwargs={"dt": dt_arg})

    commit_op = LakeFSCommitOperator(
        task_id=f"{transform_name}_commit",
        repo="example-repo",
        branch=f"{transform_name}_{dt_arg}",
        lakefs_conn_id="lakefs",
        msg="Transform result",
        metadata={},
    )

    merge_op = LakeFSMergeOperator(
        task_id=f"{transform_name}_merge",
        repo="example-repo",
        lakefs_conn_id="lakefs",
        source_ref=f"{transform_name}_{dt_arg}",
        destination_branch="main",
        msg="Merge transform result",
        metadata={},
    )
    return (merge_op << commit_op << transform_op << create_branch_op)


def transform_total_by_user(dt, **context):
    obj = s3.get_object(Bucket="example-repo", Key=f"main/raw_data/{dt}/values.csv")
    df = pd.read_csv(obj['Body'])
    df = df.groupby('user_id').agg({'event_value': 'sum'})
    _save_data_frame("total_by_user", dt, df, branch=f"total_by_user_{dt}")


def transform_total_by_event_type(dt, **context):
    obj = s3.get_object(Bucket="example-repo", Key=f"main/raw_data/{dt}/values.csv")
    df = pd.read_csv(obj['Body'])
    df = df.groupby('event_type').agg({'event_value': 'sum'})
    _save_data_frame("total_by_event_type", dt, df, branch=f"total_by_event_type_{dt}")


def transform_total_by_month(dt, **context):
    obj = s3.get_object(Bucket="example-repo", Key=f"main/raw_data/{dt}/values.csv")
    df = pd.read_csv(obj['Body'])
    df['month'] = pd.to_datetime(df['event_time']).dt.to_period('M')
    df = df.groupby('month').agg({'event_value': 'sum'})
    _save_data_frame("total_by_month", dt, df, branch=f"total_by_month_{dt}")


with DAG(
    dag_id="etl",
    catchup=False,
    schedule="@daily",
    start_date=datetime(2021, 1, 1),
    tags=["produces", "dataset-scheduled"],
) as dag1:
    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = LakeFSCommitOperator(
        task_id="commit",
        repo="example-repo",
        branch="main",
        lakefs_conn_id="lakefs",
        msg="Extract result",
        metadata={},
    )
    t3 = _transform_steps(t1.output, "total_by_user", transform_total_by_user)
    t4 = _transform_steps(t1.output, "total_by_event_type", transform_total_by_event_type)
    t5 = _transform_steps(t1.output, "total_by_month", transform_total_by_month)
    t1 >> t2 >> [t3, t4, t5]
