"""
General purpose tasks for dumping database data.
"""
from pathlib import Path
from typing import Dict, Union

from prefect import task
import basedosdados as bd
import pandas as pd

from pipelines.emd.db_dump.db import (
    Database,
    MySql,
    SqlServer,
)
from pipelines.utils import log
from pipelines.emd.db_dump.utils import (
    batch_to_dataframe,
    dataframe_to_csv,
)

DATABASE_MAPPING: Dict[str, Database] = {
    "mysql": MySql,
    "sql_server": SqlServer,
}


# pylint: disable=too-many-arguments
@task(checkpoint=False)
def database_get(
    database_type: str,
    hostname: str,
    port: int,
    user: str,
    password: str,
    database: str
) -> Database:
    """
    Returns a database object.

    Args:
        database_type: The type of the database.
        hostname: The hostname of the database.
        port: The port of the database.
        user: The username of the database.
        password: The password of the database.
        database: The database name.

    Returns:
        A database object.
    """
    if database_type not in DATABASE_MAPPING:
        raise ValueError(f"Unknown database type: {database_type}")
    return DATABASE_MAPPING[database_type](
        hostname=hostname,
        port=port,
        user=user,
        password=password,
        database=database,
    )


@task(checkpoint=False)
def database_execute(
    database: Database,
    query: str,
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """
    Executes a query on the database.

    Args:
        database: The database object.
        query: The query to execute.
    """
    database.execute_query(query)


###############
#
# File
#
###############


@task
def dump_batches_to_csv(
    database: Database,
    batch_size: int,
    prepath: Union[str, Path],
    wait=None,  # pylint: disable=unused-argument
) -> Path:
    """
    Dumps batches of data to CSV.
    """
    # Get columns
    columns = database.get_columns()
    log(f"Got columns: {columns}")

    prepath = Path(prepath)
    log(f"Got prepath: {prepath}")

    # Dump batches
    batch = database.fetch_batch(batch_size)
    idx = 0
    while len(batch) > 0:
        log(f"Dumping batch {idx} with size {len(batch)}")
        # Convert to dataframe
        dataframe = batch_to_dataframe(batch, columns)
        # Write to CSV
        dataframe_to_csv(dataframe, prepath / f"{idx}.csv")
        # Get next batch
        batch = database.fetch_batch(batch_size)
        idx += 1

    return prepath


@task
def dump_header_to_csv(
    database: Database,
    header_path: Union[str, Path],
    wait=None,  # pylint: disable=unused-argument
) -> Path:
    """
    Dumps the header to CSV.
    """
    columns = database.get_columns()
    first_row = database.fetch_batch(1)
    dataframe = pd.DataFrame(data=first_row, columns=columns)

    header_path = Path(header_path)
    dataframe_to_csv(dataframe, header_path / "header.csv")

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
        st.delete_table(
            mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
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
    # pylint: disable=C0103
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)

    # pylint: disable=C0103
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    if tb.table_exists(mode="staging"):
        st.delete_table(
            mode="staging", bucket_name=st.bucket_name, not_found_ok=True)

    tb.create(
        path=path,
        if_storage_data_exists="replace",
        if_table_config_exists="replace",
        if_table_exists="replace",
        location="southamerica-east1",
    )

    log(f"Sucessfully created table {st.bucket_name}.{dataset_id}.{table_id}")
    st.delete_table(mode="staging", bucket_name=st.bucket_name,
                    not_found_ok=True)
    log(
        f"Sucessfully remove table data from {st.bucket_name}.{dataset_id}.{table_id}")
