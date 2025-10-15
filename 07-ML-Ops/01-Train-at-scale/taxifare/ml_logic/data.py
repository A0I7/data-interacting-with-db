from taxifare.utils import simple_time_and_memory_tracker

import pandas as pd

from google.cloud import bigquery
from colorama import Fore, Style
from pathlib import Path

from taxifare.params import *
@simple_time_and_memory_tracker

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean raw data by
    - assigning correct dtypes to each column
    - removing buggy or irrelevant transactions
    """
    # Compress raw_data by setting types to DTYPES_RAW
    # $CODE_BEGIN
    df = df.astype(DTYPES_RAW)
    # $CODE_END

    # CODE_BEGIN
    # Remove buggy transactions (optimized)
    df = df.drop_duplicates().dropna(subset=[
        "pickup_longitude", "pickup_latitude",
        "dropoff_longitude", "dropoff_latitude",
        "fare_amount", "passenger_count"
    ])

    df = df.query(
        "fare_amount > 0 and passenger_count > 0 and "
        "pickup_longitude != 0 and pickup_latitude != 0 and "
        "dropoff_longitude != 0 and dropoff_latitude != 0"
    )
    # CODE_END


    # Remove geographically irrelevant transactions (rows)
    # $CODE_BEGIN
    df = df[df.fare_amount < 400]
    df = df[df.passenger_count < 8]

    df = df[df["pickup_latitude"].between(left=40.5, right=40.9)]
    df = df[df["dropoff_latitude"].between(left=40.5, right=40.9)]
    df = df[df["pickup_longitude"].between(left=-74.3, right=-73.7)]
    df = df[df["dropoff_longitude"].between(left=-74.3, right=-73.7)]
    # $CODE_END

    print("✅ data cleaned")

    return df
