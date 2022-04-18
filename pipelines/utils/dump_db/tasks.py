"""
General purpose tasks for dumping database data.
"""
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Union
from uuid import uuid4

from prefect import task

from pipelines.utils.dump_db.db import (
    Database,
    MySql,
    Oracle,
    SqlServer,
)
from pipelines.utils.utils import (
    batch_to_dataframe,
    dataframe_to_csv,
    clean_dataframe,
)
from pipelines.constants import constants
from pipelines.utils.utils import log

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
    max_retries=constants.TASK_MAX_RETRIES.value,
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
    max_retries=constants.TASK_MAX_RETRIES.value,
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


@task(
    checkpoint=False,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def format_partitioned_query(
    query: str,
    dataset_id: str,
    table_id: str,
    partition_column: str = None,
):
    """
    Formats a query for fetching partitioned data.
    """
    # If no partition column is specified, return the query as is.
    if partition_column is None:
        return query

    # Check if the table already exists in BigQuery.
    table_exists: bool = False

    # If it doesn't, return the query as is, so we can fetch the whole table.
    if not table_exists:
        return query

    # If it does, get the last partition date.
    # TODO.
    last_partition_date = None

    # Using the last partition date, get the partitioned query.
    return f"""
    with a{uuid4().hex} as ({query})
    select * from a{uuid4().hex}
    where {partition_column} >= '{last_partition_date}'
    """

###############
#
# File
#
###############


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
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
