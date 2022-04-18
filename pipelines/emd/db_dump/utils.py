"""
Utilities for `db_dump` tasks.
"""

from pathlib import Path
from typing import Tuple, List, Union

import pandas as pd

from pipelines.utils import log

###############
#
# SQL Server
#
###############


def sql_server_get_columns(cursor):
    """
    Returns the column names of the SQL Server.
    """
    log("Getting column names")
    return [column[0] for column in cursor.description]


def sql_server_fetch_batch(cursor, batch_size):
    """
    Fetches a batch of rows from the SQL Server.
    """
    log(f"Fetching batch of {batch_size} rows")
    return cursor.fetchmany(batch_size)

###############
#
# Dataframe
#
###############


def batch_to_dataframe(batch: Tuple[Tuple], columns: List[str]) -> pd.DataFrame:
    """
    Converts a batch of rows to a dataframe.
    """
    log(f"Converting batch of size {len(batch)} to dataframe")
    return pd.DataFrame(batch, columns=columns)

###############
#
# File
#
###############


def dataframe_to_csv(dataframe: pd.DataFrame, path: Union[str, Path]) -> None:
    """
    Writes a dataframe to a CSV file.
    """
    # Remove filename from path
    path = Path(path).parent
    # Create directory if it doesn't exist
    path.mkdir(parents=True, exist_ok=True)
    # Write dataframe to CSV
    log(f"Writing dataframe to CSV: {path}")
    dataframe.to_csv(path, index=False)
    log(f"Wrote dataframe to CSV: {path}")
