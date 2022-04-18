"""
General purpose tasks for dumping database data.
"""

from pathlib import Path
from typing import List, Tuple, Union

import pandas as pd
from prefect import task
import pymssql

from pipelines.utils import log

###############
#
# SQL Server
#
###############


@task
def sql_server_get_connection(server: str, user: str, password: str, database: str):
    """
    Returns a connection to the SQL Server.
    """
    log(f"Connecting to SQL Server: {server}")
    return pymssql.connect(server=server, user=user, password=password, database=database)


@task
def sql_server_get_cursor(connection):
    """
    Returns a cursor for the SQL Server.
    """
    log("Getting cursor")
    return connection.cursor()


@task
def sql_server_execute(cursor, query):
    """
    Executes a query on the SQL Server.
    """
    log(f"Executing query: {query}")
    cursor.execute(query)
    return cursor


@task
def sql_server_fetch_batch(cursor, batch_size):
    """
    Fetches a batch of rows from the SQL Server.
    """
    log(f"Fetching batch of {batch_size} rows")
    return cursor.fetchmany(batch_size)


@task
def sql_server_get_columns(cursor):
    """
    Returns the column names of the SQL Server.
    """
    log("Getting column names")
    return [column[0] for column in cursor.description]


###############
#
# Dataframe
#
###############


@task
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


@task
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


@task
def dump_batches_to_csv(cursor, batch_size: int, prepath: Union[str, Path]) -> Path:
    """
    Dumps batches of data to CSV.
    """
    # Get columns
    columns = sql_server_get_columns(cursor)
    log(f"Got columns: {columns}")

    prepath = Path(prepath)
    log(f"Got prepath: {prepath}")

    # Dump batches
    batch = sql_server_fetch_batch(cursor, batch_size)
    idx = 0
    while len(batch) > 0:
        log(f"Dumping batch {idx} with size {len(batch)}")
        # Convert to dataframe
        dataframe = batch_to_dataframe(batch, columns)
        # Write to CSV
        dataframe_to_csv(dataframe, prepath / f"{idx}.csv")
        # Get next batch
        batch = sql_server_fetch_batch(cursor, batch_size)
        idx += 1

    return prepath
