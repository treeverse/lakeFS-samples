from io import BytesIO
import pandas as pd
import boto3


lakefs_client = boto3.client('s3',
                             endpoint_url='http://lakefs:8000/',
                             aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
                             aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
                             )


def read_from_csv(table_name, dt, branch="main"):
    obj = lakefs_client.get_object(
        Bucket="example-repo",
        Key=f"{branch}/{table_name}/{dt}/values.csv"
    )
    return pd.read_csv(obj['Body'])


def save_as_csv(table_name, dt, df, branch="main"):
    csv = df.to_csv().encode('utf-8')
    lakefs_client.put_object(
        Bucket="example-repo",
        Key=f"{branch}/{table_name}/{dt}/values.csv",
        Body=BytesIO(csv),
        ContentLength=len(csv),
        ContentType='application/csv'
    )
