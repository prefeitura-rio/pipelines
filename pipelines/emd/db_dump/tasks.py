"""
General purpose tasks for dumping database data.
"""

from pathlib import Path
from typing import List, Tuple, Union

import pandas as pd
from prefect import task
import pymssql
import basedosdados as bd

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
    # pylint: disable=E1101
    return pymssql.connect(
        server=server, user=user, password=password, database=database
    )


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


###############
#
# Upload to GCS
#
###############
@task
def upload_to_gcs(path: Union[str, Path], dataset_id: str, table_id: str) -> None:
    """
    Uploads a bunch of CSVs using BD+
    """
    # pylint: disable=C0103
    tb = bd.Table(dataset_id=dataset_id,
                  table_id=table_id)
    if tb.table_exists(mode="staging"):
        # the name of the files need to be the same or the data doesn't get overwritten
        tb.append(
            filepath=path,
            if_exists="replace",
            timeout=600,
            chunk_size=1024 ** 2 * 10,
        )

        log(
            f"Successfully uploaded {path} to {tb.bucket_name}.staging.{dataset_id}.{table_id}"
        )

    else:
        # pylint: disable=C0301
        log(
            "Table does not exist in STAGING, need to create it in local first.\nCreate and publish the table in BigQuery first."
        )
