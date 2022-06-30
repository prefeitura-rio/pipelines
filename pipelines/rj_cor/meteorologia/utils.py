# -*- coding: utf-8 -*-
import pandas as pd
from redis_pal import RedisPal

from pipelines.utils.utils import (
    get_redis_client,
)


def save_updated_rows_on_redis(
    df: pd.DataFrame, dataset_id: str, table_id: str, mode: str = "prod"
) -> pd.DataFrame:
    """
    Acess redis to get the last time each id_estacao was updated, return
    updated stations as a DataFrame and save new dates on redis
    """

    redis_client = get_redis_client()

    key = dataset_id + "." + table_id
    if mode == "dev":
        key = f"{mode}.{key}"

    # Access all data saved on redis with this key
    updates = redis_client.hgetall(key)

    # Convert data in dictionary in format with id_estacao in key and last updated time as value
    # Example > {"12": "2022-06-06 14:45:00"}
    updates = {k.decode("utf-8"): v.decode("utf-8") for k, v in updates.items()}

    # Convert dictionary to df
    updates = pd.DataFrame(updates.items(), columns=["id_estacao", "last_update"])

    # df and updates need to have the same index, in our case id_estacao
    missing_in_df = [
        i for i in updates.id_estacao.unique() if i not in df.id_estacao.unique()
    ]
    missing_in_updates = [
        i for i in df.id_estacao.unique() if i not in updates.id_estacao.unique()
    ]

    # If id_estacao doesn't exists on updates we create a fake date for this station on updates
    if len(missing_in_updates) > 0:
        for i in missing_in_updates:
            updates = updates.append(
                {"id_estacao": i, "last_update": "1900-01-01 00:00:00"},
                ignore_index=True,
            )

    # If id_estacao doesn't exists on df we remove this stations from updates
    if len(missing_in_df) > 0:
        updates = updates[~updates.id_estacao.isin(missing_in_df)]

    # Set the index with the id_estacao
    df.set_index(df.id_estacao.unique(), inplace=True)
    updates.set_index(updates.id_estacao.unique(), inplace=True)

    # Keep on df only the stations that has a time after the one that is saved on redis
    df = df.where(
        (df.id_estacao == updates.id_estacao) & (df.data_medicao > updates.last_update)
    ).dropna(subset=["id_estacao"])

    # Convert stations with the new updates dates in a dictionary
    df.set_index("id_estacao", inplace=True)
    new_updates = df["data_medicao"].astype(str).to_dict()

    # Save this new information on redis
    [redis_client.hset(key, k, v) for k, v in new_updates.items()]

    return df.reset_index()
