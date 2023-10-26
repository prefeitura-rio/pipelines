# -*- coding: utf-8 -*-
"""
General purpose tasks for dumping MongoDB data.
"""
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union
from uuid import uuid4

import pandas as pd
from prefect import task

from pipelines.utils.dump_mongo.mongo import Mongo
from pipelines.utils.utils import (
    dataframe_to_csv,
    dataframe_to_parquet,
    parse_date_columns,
    clean_dataframe,
    to_partitions,
    remove_columns_accents,
)
from pipelines.constants import constants
from pipelines.utils.utils import log


@task(
    checkpoint=False,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def database_get(
    hostname: str,
    user: str,
    password: str,
    database: str,
    collection: str,
    port: int = 27017,
) -> Mongo:
    """
    Returns a Mongo object.

    Args:
        hostname: Hostname of the database.
        user: Username of the database.
        password: Password of the database.
        database: Database name.
        collection: Collection name.
        port: Port of the database.

    Returns:
        A database object.
    """
    return Mongo(
        hostname=hostname,
        user=user,
        password=password,
        database=database,
        collection=collection,
        port=port,
    )


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
    nout=2,
)
def dump_batches_to_file(
    database: Mongo,
    batch_size: int,
    prepath: Union[str, Path],
    date_field: str = None,
    date_lower_bound: datetime = None,
    batch_data_type: str = "csv",
) -> Path:
    """
    Dumps batches of data to FILE.
    """
    # Dump batches
    batch = database.fetch_batch(batch_size, date_field, date_lower_bound)
    idx = 0
    while len(batch) > 0:
        if idx % 100 == 0:
            log(f"Dumping batch {idx} with size {len(batch)}")
        # Batch -> DataFrame
        dataframe: pd.DataFrame = pd.DataFrame(batch)
        # Clean DataFrame
        old_columns = dataframe.columns.tolist()
        dataframe.columns = remove_columns_accents(dataframe)
        new_columns_dict = dict(zip(old_columns, dataframe.columns.tolist()))
        dataframe = clean_dataframe(dataframe)
        # DataFrame -> File
        if date_field:
            dataframe, date_partition_columns = parse_date_columns(
                dataframe, new_columns_dict[date_field]
            )
            to_partitions(
                data=dataframe,
                partition_columns=date_partition_columns,
                savepath=prepath,
                data_type=batch_data_type,
            )
        elif batch_data_type == "csv":
            dataframe_to_csv(dataframe, prepath / f"{uuid4()}.csv")
        elif batch_data_type == "parquet":
            dataframe_to_parquet(dataframe, prepath / f"{uuid4()}.parquet")
        # Get next batch
        batch = database.fetch_batch(batch_size, date_field, date_lower_bound)
        idx += 1

    log(f"Successfully dumped {idx} batches with size {len(batch)}")

    return prepath, idx
