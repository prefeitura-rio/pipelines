# -*- coding: utf-8 -*-
"""
General purpose tasks for dumping data from URLs.
"""
from typing import List

import pandas as pd
from pipelines.utils.utils import (
    log,
    remove_columns_accents,
    parse_date_columns,
    to_partitions,
    clean_dataframe,
    dataframe_to_csv,
)


def handle_dataframe_chunk(
    dataframe: pd.DataFrame,
    save_path: str,
    partition_columns: List[str],
    event_id: str,
    idx: int,
    build_json_dataframe: bool = False,
    dataframe_key_column: str = None,
):
    """
    Handles a chunk of dataframe.
    """
    if not partition_columns or partition_columns[0] == "":
        partition_column = None
    else:
        partition_column = partition_columns[0]

    if not partition_column:
        log("NO partition column specified! Writing unique files")
    else:
        log(f"Partition column: {partition_column} FOUND!! Write to partitioned files")

    dataframe.columns = remove_columns_accents(dataframe)

    old_columns = dataframe.columns.tolist()
    dataframe.columns = remove_columns_accents(dataframe)
    new_columns_dict = dict(zip(old_columns, dataframe.columns.tolist()))
    if idx == 0:
        log(f"New columns without accents: {new_columns_dict}")

    dataframe = clean_dataframe(dataframe)

    if partition_column:
        dataframe, date_partition_columns = parse_date_columns(
            dataframe, new_columns_dict[partition_column]
        )

        partitions = date_partition_columns + [
            new_columns_dict[col] for col in partition_columns[1:]
        ]
        to_partitions(
            data=dataframe,
            partition_columns=partitions,
            savepath=save_path,
            data_type="csv",
            build_json_dataframe=build_json_dataframe,
            dataframe_key_column=dataframe_key_column,
        )
    else:
        dataframe_to_csv(
            dataframe,
            save_path / f"{event_id}-{idx}.csv",
            build_json_dataframe=build_json_dataframe,
            dataframe_key_column=dataframe_key_column,
        )
