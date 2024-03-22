# -*- coding: utf-8 -*-
"""
General purpose tasks for dumping data from URLs.
"""
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import pandas as pd
from prefect.schedules.clocks import IntervalClock
from pipelines.utils.utils import (
    log,
    remove_columns_accents,
    parse_date_columns,
    to_partitions,
    clean_dataframe,
    dataframe_to_csv,
)


# pylint: disable=R0913
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

    old_columns = dataframe.columns.tolist()
    dataframe.columns = remove_columns_accents(dataframe)
    new_columns_dict = dict(zip(old_columns, dataframe.columns.tolist()))
    if idx == 0:
        if partition_column:
            log(
                f"Partition column: {partition_column} FOUND!! Write to partitioned files"
            )

        else:
            log("NO partition column specified! Writing unique files")

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
            dataframe=dataframe,
            path=Path(save_path) / f"{event_id}-{idx}.csv",
            build_json_dataframe=build_json_dataframe,
            dataframe_key_column=dataframe_key_column,
        )


def generate_dump_url_schedules(  # pylint: disable=too-many-arguments,too-many-locals
    interval: timedelta,
    start_date: datetime,
    labels: List[str],
    # db_database: str,
    # db_host: str,
    # db_port: Union[str, int],
    # db_type: str,
    dataset_id: str,
    # vault_secret_path: str,
    table_parameters: dict,
    batch_data_type: str = "csv",
    runs_interval_minutes: int = 15,
) -> List[IntervalClock]:
    """
    Generates multiple schedules for url dumping.
    """
    # db_port = str(db_port)
    clocks = []
    for count, (table_id, parameters) in enumerate(table_parameters.items()):
        parameter_defaults = {
            "batch_data_type": batch_data_type,
            "url": parameters["url"],
            "url_type": parameters["url_type"],
            "dataset_id": dataset_id if dataset_id != "" else parameters["dataset_id"],
            "table_id": table_id,
            "dump_mode": parameters["dump_mode"],
            # "vault_secret_path": vault_secret_path,
            # "db_database": db_database,
            # "db_host": db_host,
            # "db_port": db_port,
            # "db_type": db_type,
            # "execute_query": query_to_line(parameters["execute_query"]),
        }
        if "gsheets_sheet_order" in parameters:
            parameter_defaults["gsheets_sheet_order"] = parameters[
                "gsheets_sheet_order"
            ]
        if "gsheets_sheet_name" in parameters:
            parameter_defaults["gsheets_sheet_name"] = parameters["gsheets_sheet_name"]
        if "gsheets_sheet_range" in parameters:
            parameter_defaults["gsheets_sheet_range"] = parameters[
                "gsheets_sheet_range"
            ]
        if "partition_columns" in parameters:
            parameter_defaults["partition_columns"] = parameters["partition_columns"]
        if "materialize_after_dump" in parameters:
            parameter_defaults["materialize_after_dump"] = parameters[
                "materialize_after_dump"
            ]
        if "materialization_mode" in parameters:
            parameter_defaults["materialization_mode"] = parameters[
                "materialization_mode"
            ]
        if "materialize_to_datario" in parameters:
            parameter_defaults["materialize_to_datario"] = parameters[
                "materialize_to_datario"
            ]
        if "encoding" in parameters:
            parameter_defaults["encoding"] = parameters["encoding"]
        if "on_bad_lines" in parameters:
            parameter_defaults["on_bad_lines"] = parameters["on_bad_lines"]
        if "separator" in parameters:
            parameter_defaults["separator"] = parameters["separator"]
        # if "dbt_model_secret_parameters" in parameters:
        #     parameter_defaults["dbt_model_secret_parameters"] = parameters[
        #         "dbt_model_secret_parameters"
        #     ]
        # if "partition_date_format" in parameters:
        #     parameter_defaults["partition_date_format"] = parameters[
        #         "partition_date_format"
        #     ]
        # if "lower_bound_date" in parameters:
        #     parameter_defaults["lower_bound_date"] = parameters["lower_bound_date"]

        new_interval = parameters["interval"] if "interval" in parameters else interval

        clocks.append(
            IntervalClock(
                interval=new_interval,
                start_date=start_date
                + timedelta(minutes=runs_interval_minutes * count),
                labels=labels,
                parameter_defaults=parameter_defaults,
            )
        )
    return clocks
