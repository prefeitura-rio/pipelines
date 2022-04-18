"""
General purpose tasks for dumping database data.
"""
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Union
from uuid import uuid4

import basedosdados as bd
from prefect import task

from pipelines.utils.dump_db.db import (
    Database,
    MySql,
    Oracle,
    SqlServer,
)
from pipelines.utils.dump_db.utils import (
    extract_last_partition_date,
    parse_date_columns,
)
from pipelines.utils.utils import (
    batch_to_dataframe,
    dataframe_to_csv,
    clean_dataframe,
    to_partitions,
    parser_blobs_to_partition_dict,
    get_storage_blobs,
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
    wait=None,  # pylint: disable=unused-argument
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
    log(f"Executing query: {query}")
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
    lower_bound_date: str = None,
    wait=None,  # pylint: disable=unused-argument
):
    """
    Formats a query for fetching partitioned data.
    """
    # If no partition column is specified, return the query as is.
    if partition_column is None:
        log("No partition column specified. Returning query as is.")
        return query

    # Check if the table already exists in BigQuery.
    table = bd.Table(dataset_id, table_id)

    # If it doesn't, return the query as is, so we can fetch the whole table.
    if not table.table_exists("staging"):
        log("No table was found. Returning query as is.")
        return query

    blobs = get_storage_blobs(dataset_id, table_id)

    # extract only partitioned folders
    storage_partitions_dict = parser_blobs_to_partition_dict(blobs)
    # get last partition date
    last_partition_date = extract_last_partition_date(storage_partitions_dict)

    if lower_bound_date:
        last_date = min(lower_bound_date, last_partition_date)
    else:
        last_date = last_partition_date

    # Using the last partition date, get the partitioned query.
    # `aux_name` must be unique and start with a letter, for better compatibility with
    # multiple DBMSs.
    aux_name = f"a{uuid4().hex}"

    log(
        "Partitioned detected, retuning a NEW QUERY with partitioned columns and filters."
    )

    return f"""
    with {aux_name} as ({query})
    select * from {aux_name}
    where {partition_column} >= '{last_date}'
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
    partition_column: str = None,
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

    if not partition_column:
        log("NO partition column specified! Writing unique files")
    else:
        log(f"Partition column: {partition_column} FOUND!! Write to partitioned files")
    # Dump batches
    batch = database.fetch_batch(batch_size)
    eventid = datetime.now().strftime("%Y%m%d-%H%M%S")
    idx = 0
    while len(batch) > 0:
        if idx % 100 == 0:
            log(f"Dumping batch {idx} with size {len(batch)}")
        # Convert to dataframe
        dataframe = batch_to_dataframe(batch, columns)
        # Clean dataframe
        dataframe = clean_dataframe(dataframe)
        # Write to CSV
        if not partition_column:
            dataframe_to_csv(dataframe, prepath / f"{eventid}-{idx}.csv")
        else:
            dataframe, partition_columns = parse_date_columns(
                dataframe, partition_column
            )
            to_partitions(dataframe, partition_columns, prepath)
        # Get next batch
        batch = database.fetch_batch(batch_size)
        idx += 1

    log(f"Dumped {idx} batchs with size {len(batch)}, total of {idx*batch_size}")

    return prepath
