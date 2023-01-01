from datetime import datetime
from io import BytesIO

import boto3
import numpy as np
import pandas as pd
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from lakefs_provider.operators.commit_operator import LakeFSCommitOperator

# [START dataset_def]
dag1_dataset = Dataset("s3://dag1/output_1.txt", extra={"hi": "bye"})
# [END dataset_def]
dag2_dataset = Dataset("s3://dag2/output_1.txt", extra={"hi": "bye"})


# Extracts raw data and uploads it to lakeFS
def extract():
    np.random.seed(datetime.now().microsecond)  # TODO - remove this usage of seed
    df = pd.DataFrame({
        'a': np.random.rand(100000),
        'b': np.random.rand(100000),
    })
    csv = df.to_csv().encode('utf-8')
    s3 = boto3.client('s3',
                      endpoint_url='http://lakefs:8000/',
                      aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
                      aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
                      )
    s3.put_object(
        Bucket="example-repo",
        Key="main/raw_data/test.csv",
        Body=BytesIO(csv),
        ContentLength=len(csv),
        ContentType='application/csv'
    )


def transform():
    s3 = boto3.client('s3',
                      endpoint_url='http://lakefs:8000/',
                      aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
                      aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
                      )
    obj = s3.get_object(Bucket="example-repo", Key="main/raw_data/test.csv")
    df = pd.read_csv(obj['Body'])
    df['c'] = df['a'] + df['b']
    csv = df.to_csv().encode('utf-8')
    s3.put_object(
        Bucket="example-repo",
        Key="main/transformed/test.csv",
        Body=BytesIO(csv),
        ContentLength=len(csv),
        ContentType='application/csv'
    )


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
    t3 = PythonOperator(task_id="transform", python_callable=transform)
    t4 = LakeFSCommitOperator(
        task_id="commit2",
        repo="example-repo",
        branch="main",
        lakefs_conn_id="lakefs",
        msg="Transform result",
        metadata={},
    )
    t1 >> t2 >> t3 >> t4
