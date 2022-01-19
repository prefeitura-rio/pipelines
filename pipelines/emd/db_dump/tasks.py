"""
General purpose tasks for dumping database data.
"""
from email import header
import os
from pathlib import Path
from typing import Union

from prefect import task
import pymssql
import basedosdados as bd
import pandas as pd

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


@task
def sql_server_log_headers(cursor) -> None:
    """
    Logs the headers of the SQL Server table.
    """
    log("Getting headers")
    log(f"Columns: {sql_server_get_columns(cursor)}")
    log(f"First line: {cursor.fetchone()}")


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


@task
def dump_header_to_csv(cursor, header_path: Union[str, Path]) -> Path:
    columns = sql_server_get_columns(cursor)
    data = [cursor.fetchone()]
    df = pd.DataFrame(data=data, columns=columns)

    header_path = Path(header_path)
    dataframe_to_csv(df, header_path / "header.csv")

    log(f"Wrote header to {header_path}")

    return header_path


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
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)

    if tb.table_exists(mode="staging"):
        # Delete old data
        st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
        log(
            f"Successfully deleted OLD DATA {st.bucket_name}.staging.{dataset_id}.{table_id}"
        )

        # the name of the files need to be the same or the data doesn't get overwritten
        tb.append(
            filepath=path,
            if_exists="replace",
        )

        log(
            f"Successfully uploaded {path} to {tb.bucket_name}.staging.{dataset_id}.{table_id}"
        )

    else:
        # pylint: disable=C0301
        log(
            "Table does not exist in STAGING, need to create it in local first.\nCreate and publish the table in BigQuery first."
        )


@task
def create_bd_table(path: Union[str, Path], dataset_id: str, table_id: str) -> None:
    """
    Create table using BD+
    """
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)

    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    if tb.table_exists(mode="staging"):
        st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)

    tb.create(
        path=path,
        if_storage_data_exists="replace",
        if_table_config_exists="replace",
        if_table_exists="replace",
        location="southamerica-east1",
    )

    log(f"Sucessfully created table {st.bucket_name}.{dataset_id}.{table_id}")
    st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
    log(f"Sucessfully remove table data from {st.bucket_name}.{dataset_id}.{table_id}")
