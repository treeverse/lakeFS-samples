from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import s3_utils

RAW_ROW_COUNT = 100000

np.random.seed(datetime.now().microsecond)


def _random_dates():
    deltas = np.random.randint(0, 60 * 60 * 24 * 365, RAW_ROW_COUNT)
    base = datetime.now() - timedelta(days=365)
    return [datetime.fromtimestamp(base.timestamp() + delta).isoformat() for delta in deltas]


# Extracts raw data and uploads it to lakeFS
def extract():
    df = pd.DataFrame({
        'user_id': np.random.randint(1, 1000, RAW_ROW_COUNT),
        'event_value': np.random.rand(RAW_ROW_COUNT),
        'event_type': np.random.choice(['click', 'purchase', 'view'], RAW_ROW_COUNT),
        'event_time': pd.array(_random_dates(), dtype="string"),
    })
    dt = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    s3_utils.save_as_csv("raw_data", dt, df)
    return dt
