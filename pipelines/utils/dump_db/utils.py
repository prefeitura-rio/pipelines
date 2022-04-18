# -*- coding: utf-8 -*-
"""
Utilities for the Database Dump flows.
"""

from datetime import datetime, timedelta
from typing import List, Tuple, Union

import pandas as pd

from prefect.schedules.clocks import IntervalClock
from pipelines.utils.utils import (
    log,
    query_to_line,
    remove_columns_accents,
)


def extract_last_partition_date(partitions_dict: dict):
    """
    Extract last date from partitions folders
    """
    last_partition_date = None
    for partition, values in partitions_dict.items():
        try:
            last_partition_date = datetime.strptime(max(values), "%Y-%m-%d").strftime(
                "%Y-%m-%d"
            )
            log(f"{partition} is in date format Y-m-d")
        except ValueError:
            log(f"Partition {partition} is not a date")
    return last_partition_date


def parse_date_columns(
    dataframe: pd.DataFrame, partition_date_column: str
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Parses the date columns to the partition format.
    """
    ano_col = "ano_particao"
    mes_col = "mes_particao"
    data_col = "data_particao"
    cols = [ano_col, mes_col, data_col]
    for col in cols:
        if col in dataframe.columns:
            raise ValueError(f"Column {col} already exists, please review your model.")

    dataframe[data_col] = pd.to_datetime(dataframe[partition_date_column])
    dataframe[ano_col] = dataframe[data_col].dt.year
    dataframe[mes_col] = dataframe[data_col].dt.month
    dataframe[data_col] = dataframe[data_col].dt.date

    return dataframe, [ano_col, mes_col, data_col]


def build_query_new_columns(table_columns: List[str]) -> List[str]:
    """ "
    Creates the query without accents.
    """
    new_cols = remove_columns_accents(pd.DataFrame(columns=table_columns))
    return "\n".join(
        [
            f"{old_col} AS {new_col},"
            for old_col, new_col in zip(table_columns, new_cols)
        ]
    )


def generate_dump_db_schedules(  # pylint: disable=too-many-arguments,too-many-locals
    interval: timedelta,
    start_date: datetime,
    labels: List[str],
    db_database: str,
    db_host: str,
    db_port: Union[str, int],
    db_type: str,
    dataset_id: str,
    vault_secret_path: str,
    table_parameters: dict,
    batch_size: int = 50000,
    runs_interval_minutes: int = 15,
) -> List[IntervalClock]:
    """
    Generates multiple schedules for database dumping.
    """
    db_port = str(db_port)
    clocks = []
    for count, (table_id, parameters) in enumerate(table_parameters.items()):
        parameter_defaults = {
            "batch_size": batch_size,
            "vault_secret_path": vault_secret_path,
            "db_database": db_database,
            "db_host": db_host,
            "db_port": db_port,
            "db_type": db_type,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "dump_type": parameters["dump_type"],
            "execute_query": query_to_line(parameters["execute_query"]),
        }
        if "partition_column" in parameters:
            parameter_defaults["partition_column"] = parameters["partition_column"]
        if "lower_bound_date" in parameters:
            parameter_defaults["lower_bound_date"] = parameters["lower_bound_date"]
        if "materialize_after_dump" in parameters:
            parameter_defaults["materialize_after_dump"] = parameters[
                "materialize_after_dump"
            ]
        if "materialization_mode" in parameters:
            parameter_defaults["materialization_mode"] = parameters[
                "materialization_mode"
            ]

        clocks.append(
            IntervalClock(
                interval=interval,
                start_date=start_date
                + timedelta(minutes=runs_interval_minutes * count),
                labels=labels,
                parameter_defaults=parameter_defaults,
            )
        )
    return clocks
