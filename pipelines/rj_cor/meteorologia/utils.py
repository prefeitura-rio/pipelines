# -*- coding: utf-8 -*-
# pylint: disable=W0106
"""
General utilities for meteorologia.
"""
import pandas as pd

from pipelines.utils.utils import (
    get_redis_client,
)


def save_updated_rows_on_redis(
    dfr: pd.DataFrame,
    dataset_id: str,
    table_id: str,
    unique_id: str = "id_estacao",
    mode: str = "prod",
) -> pd.DataFrame:
    """
    Acess redis to get the last time each unique_id was updated, return
    updated unique_id as a DataFrame and save new dates on redis
    """

    redis_client = get_redis_client()

    key = dataset_id + "." + table_id
    if mode == "dev":
        key = f"{mode}.{key}"

    # Access all data saved on redis with this key
    updates = redis_client.hgetall(key)

    # Convert data in dictionary in format with unique_id in key and last updated time as value
    # Example > {"12": "2022-06-06 14:45:00"}
    updates = {k.decode("utf-8"): v.decode("utf-8") for k, v in updates.items()}

    # Convert dictionary to dfr
    updates = pd.DataFrame(updates.items(), columns=[unique_id, "last_update"])
    log(f">>> data saved in redis: {updates}")
    # dfr and updates need to have the same index, in our case unique_id
    missing_in_dfr = [
        i for i in updates[unique_id].unique() if i not in dfr[unique_id].unique()
    ]
    log(f">>> data missing_in_dfr: {missing_in_dfr}")
    missing_in_updates = [
        i for i in dfr[unique_id].unique() if i not in updates[unique_id].unique()
    ]
    log(f">>> data missing_in_updates: {missing_in_updates}")
    # If unique_id doesn't exists on updates we create a fake date for this station on updates
    if len(missing_in_updates) > 0:
        for i in missing_in_updates:
            updates = updates.append(
                {unique_id: i, "last_update": "1900-01-01 00:00:00"},
                ignore_index=True,
            )

    # If unique_id doesn't exists on dfr we remove this stations from updates
    if len(missing_in_dfr) > 0:
        updates = updates[~updates[unique_id].isin(missing_in_dfr)]

    # Set the index with the unique_id
    dfr.set_index(dfr[unique_id].unique(), inplace=True)
    updates.set_index(updates[unique_id].unique(), inplace=True)
    log(f">>> new dfr: {dfr}")
    log(f">>> new updates: {updates}")
    # Keep on dfr only the stations that has a time after the one that is saved on redis
    dfr = dfr.where(
        (dfr[unique_id] == updates[unique_id])
        & (dfr.data_medicao > updates.last_update)
    ).dropna(subset=[unique_id])
    log(f">>> data to save in redis as a dataframe: {dfr}")
    # Convert stations with the new updates dates in a dictionary
    dfr.set_index(unique_id, inplace=True)
    new_updates = dfr["data_medicao"].astype(str).to_dict()
    log(f">>> data to save in redis as a dict: {new_updates}")
    # Save this new information on redis
    [redis_client.hset(key, k, v) for k, v in new_updates.items()]

    return dfr.reset_index()
