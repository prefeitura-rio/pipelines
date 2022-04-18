"""
General purpose tasks for dumping database data.
"""
from pathlib import Path
from typing import Dict, Union
from datetime import datetime, timedelta

import glob
from prefect import task
import basedosdados as bd
import pandas as pd

from pipelines.emd.db_dump.db import (
    Database,
    MySql,
    Oracle,
    SqlServer,
)
from pipelines.emd.db_dump.utils import (
    batch_to_dataframe,
    dataframe_to_csv,
    clean_dataframe,
)
from pipelines.constants import constants

DATABASE_MAPPING: Dict[str, Database] = {
    "mysql": MySql,
    "oracle": Oracle,
    "sql_server": SqlServer,
}


# pylint: disable=too-many-arguments
@task(
    checkpoint=False,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def database_get(
    database_type: str,
    hostname: str,
    port: int,
    user: str,
    password: str,
    database: str,
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


@task(
    checkpoint=False,
    max_retries=constants.constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
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


@task(
    checkpoint=False,
    max_retries=constants.constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def database_fetch(
    database: Database,
    batch_size: str,
    wait=None,  # pylint: disable=unused-argument
):
    """
    Fetches the results of a query on the database.
    """
    if batch_size == "all":
        log(f"columns: {database.get_columns()}")
        log(f"All rows: { database.fetch_all()}")
    else:
        try:
            batch_size_no = int(batch_size)
        except ValueError as error:
            raise ValueError(f"Invalid batch size: {batch_size}") from error
        log(f"columns: {database.get_columns()}")
        log(f"{batch_size_no} rows: {database.fetch_batch(batch_size_no)}")


###############
#
# File
#
###############


@task(
    max_retries=constants.constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
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
    eventid = datetime.now().strftime("%Y%m%d-%H%M%S")
    idx = 0
    while len(batch) > 0:
        log(f"Dumping batch {idx} with size {len(batch)}")
        # Convert to dataframe
        dataframe = batch_to_dataframe(batch, columns)
        # Clean dataframe
        dataframe = clean_dataframe(dataframe)
        # Write to CSV
        dataframe_to_csv(dataframe, prepath / f"{eventid}-{idx}.csv")
        # Get next batch
        batch = database.fetch_batch(batch_size)
        idx += 1

    return prepath


@task(
    max_retries=constants.constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def dump_header_to_csv(
    header_path: Union[str, Path],
    data_path: Union[str, Path],
    wait=None,  # pylint: disable=unused-argument
) -> Path:
    """
    Dumps the header to CSV.
    """
    files = glob.glob(f"{data_path}/*")
    file = files[0] if files != [] else ""

    dataframe = pd.read_csv(f"{file}", nrows=1)

    header_path = Path(header_path)
    dataframe_to_csv(dataframe, header_path / "header.csv")

    log(f"Wrote header to {header_path} from file {file}")

    return header_path


###############
#
# Upload to GCS
#
###############
@task(
    max_retries=constants.constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def upload_to_gcs(
    path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """
    Uploads a bunch of CSVs using BD+
    """
    # pylint: disable=C0103
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    # st = bd.Storage(dataset_id=dataset_id, table_id=table_id)

    if tb.table_exists(mode="staging"):
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


@task(
    max_retries=constants.constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def create_bd_table(
    path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    dump_type: str,
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """
    Create table using BD+
    """
    # pylint: disable=C0103
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)

    # pylint: disable=C0103
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)

    # full dump
    if dump_type == "append":
        if tb.table_exists(mode="staging"):
            log(
                f"Mode append: Table {st.bucket_name}.{dataset_id}.{table_id} already exists"
            )
        else:
            tb.create(
                path=path,
                location="southamerica-east1",
            )
            log(
                f"Mode append: Sucessfully created a new table {st.bucket_name}.{dataset_id}.{table_id}"
            )  # pylint: disable=C0301

            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
            log(
                f"Mode append: Sucessfully remove header data from {st.bucket_name}.{dataset_id}.{table_id}"
            )  # pylint: disable=C0301
    elif dump_type == "overwrite":
        if tb.table_exists(mode="staging"):
            log(
                f"Mode overwrite: Table {st.bucket_name}.{dataset_id}.{table_id} already exists, DELETING OLD DATA!"
            )  # pylint: disable=C0301
            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
        ## prod datasets is public if the project is datario. staging are private im both projects
        dataset_is_public = tb.client["bigquery_prod"].project == "datario"
        tb.create(
            path=path,
            if_storage_data_exists="replace",
            if_table_config_exists="replace",
            if_table_exists="replace",
            location="southamerica-east1",
            dataset_is_public=dataset_is_public,
        )

        log(
            f"Mode overwrite: Sucessfully created table {st.bucket_name}.{dataset_id}.{table_id}"
        )
        st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
        log(
            f"Mode overwrite: Sucessfully remove header data from {st.bucket_name}.{dataset_id}.{table_id}"
        )  # pylint: disable=C0301
