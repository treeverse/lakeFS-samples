import os

import pandas as pd
import s3_utils
from dulwich.repo import Repo

try:
    # This can be any code that fetches the current transform version
    r = Repo(os.getenv("TRANSFORM_CODE_GIT_ROOT"))
    git_sha = r.head()
except Exception as e:
    print(f"Failed to get git sha: {e}")
    git_sha = "unknown"


def get_version():
    return git_sha


def transform_total_by_user(dt, **context):
    df = s3_utils.read_from_csv("raw_data", dt)
    df = df.groupby('user_id').agg({'event_value': 'sum'})
    s3_utils.save_as_csv("total_by_user", dt, df, branch=f"total_by_user_{dt}")


def transform_total_by_event_type(dt, **context):
    df = s3_utils.read_from_csv("raw_data", dt)
    df = df.groupby('event_type').agg({'event_value': 'sum'})
    s3_utils.save_as_csv("total_by_event_type", dt, df,
                         branch=f"total_by_event_type_{dt}")


def transform_total_by_month(dt, **context):
    df = s3_utils.read_from_csv("raw_data", dt)
    df['month'] = pd.to_datetime(df['event_time']).dt.to_period('M')
    df = df.groupby('month').agg({'event_value': 'sum'})
    s3_utils.save_as_csv("total_by_month", dt, df,
                         branch=f"total_by_month_{dt}")
