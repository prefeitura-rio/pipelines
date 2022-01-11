"""
General purpose tasks for dumping database data.
"""

from pathlib import Path
from typing import Union

from prefect import task
import pymssql
import basedosdados as bd

from pipelines.utils import log
from pipelines.emd.db_dump.utils import (
    batch_to_dataframe,
    dataframe_to_csv,
    sql_server_fetch_batch,
    sql_server_get_columns,
)

###############
#
# SQL Server
#
###############


@task(checkpoint=False)
def sql_server_get_connection(server: str, user: str, password: str, database: str):
    """
    Returns a connection to the SQL Server.
    """
    log(f"Connecting to SQL Server: {server}")
    # pylint: disable=E1101
    return pymssql.connect(
        server=server, user=user, password=password, database=database
    )


@task(checkpoint=False)
def sql_server_get_cursor(connection):
    """
    Returns a cursor for the SQL Server.
    """
    log("Getting cursor")
    return connection.cursor()


@task(checkpoint=False)
def sql_server_execute(cursor, query):
    """
    Executes a query on the SQL Server.
    """
    log(f"Executing query: {query}")
    cursor.execute(query)
    return cursor


###############
#
# File
#
###############


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
