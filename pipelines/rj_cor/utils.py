# -*- coding: utf-8 -*-
"""
Utils for rj-cor
"""
import json
import pandas as pd
from pipelines.utils.utils import (
    get_redis_client,
    log,
    treat_redis_output,
)


def build_redis_key(dataset_id: str, table_id: str, name: str, mode: str = "prod"):
    """
    Helper function for building a key where to store the last updated time
    """
    key = mode + "." + dataset_id + "." + table_id + "." + name
    return key


def get_redis_output(redis_key, is_df: bool = False):
    """
    Get Redis output. Use get to obtain a df from redis or hgetall if is a key value pair.
    """
    redis_client = get_redis_client()  # (host="127.0.0.1")

    if is_df:
        json_data = redis_client.get(redis_key)
        log(f"[DEGUB] json_data {json_data}")
        if json_data:
            # If data is found, parse the JSON string back to a Python object (dictionary)
            data_dict = json.loads(json_data)
            # Convert the dictionary back to a DataFrame
            return pd.DataFrame(data_dict)

        return pd.DataFrame()

    output = redis_client.hgetall(redis_key)
    if len(output) > 0:
        output = treat_redis_output(output)
    return output


def compare_actual_df_with_redis_df(
    dfr: pd.DataFrame,
    dfr_redis: pd.DataFrame,
    columns: list,
) -> pd.DataFrame:
    """
    Compare df from redis to actual df and return only the rows from actual df
    that are not already saved on redis.
    """
    for col in columns:
        if col not in dfr_redis.columns:
            dfr_redis[col] = None
        dfr_redis[col] = dfr_redis[col].astype(dfr[col].dtypes)
    log(f"\nEnded conversion types from dfr to dfr_redis: \n{dfr_redis.dtypes}")

    dfr_diff = (
        pd.merge(dfr, dfr_redis, how="left", on=columns, indicator=True)
        .query('_merge == "left_only"')
        .drop("_merge", axis=1)
    )
    log(
        f"\nDf resulted from the difference between dft_redis and dfr: \n{dfr_diff.head()}"
    )

    updated_dfr_redis = pd.concat([dfr_redis, dfr_diff[columns]])

    return dfr_diff, updated_dfr_redis
