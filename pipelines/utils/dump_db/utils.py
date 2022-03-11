"""
Utilities for the Database Dump flows.
"""

from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from pipelines.utils.utils import log


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
