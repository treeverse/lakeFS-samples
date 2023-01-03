from datetime import datetime

import numpy as np
import pandas as pd
import s3_utils
from airflow.models import Variable
from dulwich.repo import Repo

np.random.seed(datetime.now().microsecond)

try:
    # This can be any code that fetches the current transform version
    r = Repo(Variable.get("transform_code_git_root", "unknown"))
    git_sha = r.head().decode("utf-8")
except Exception as e:
    print(f"Failed to get git sha: {e}")
    git_sha = "unknown"


def get_version():
    return git_sha


def dummy_validate_data():
    if np.random.rand() < 0.5:
        raise Exception("Failed transform validation")


def transform_total_by_user(dt, **context):
    df = s3_utils.read_from_csv("raw_data", dt)
    df = df.groupby('user_id').agg({'event_value': 'sum'})
    s3_utils.save_as_csv("total_by_user", dt, df, branch=f"total_by_user_{dt}")
    dummy_validate_data()


def transform_total_by_event_type(dt, **context):
    df = s3_utils.read_from_csv("raw_data", dt)
    df = df.groupby('event_type').agg({'event_value': 'sum'})
    s3_utils.save_as_csv("total_by_event_type", dt, df,
                         branch=f"total_by_event_type_{dt}")
    dummy_validate_data()


def transform_total_by_month(dt, **context):
    df = s3_utils.read_from_csv("raw_data", dt)
    df['month'] = pd.to_datetime(df['event_time']).dt.to_period('M')
    df = df.groupby('month').agg({'event_value': 'sum'})
    s3_utils.save_as_csv("total_by_month", dt, df,
                         branch=f"total_by_month_{dt}")
    dummy_validate_data()
