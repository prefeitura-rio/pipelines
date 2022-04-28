# -*- coding: utf-8 -*-
"""
General purpose tasks for dumping database data.
"""
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Union
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
    build_query_new_columns,
)
from pipelines.utils.utils import (
    batch_to_dataframe,
    dataframe_to_csv,
    clean_dataframe,
    to_partitions,
    parser_blobs_to_partition_dict,
    get_storage_blobs,
    remove_columns_accents,
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
    partition_columns: List[str] = None,
    lower_bound_date: str = None,
    date_format: str = None,
    wait=None,  # pylint: disable=unused-argument
):
    """
    Formats a query for fetching partitioned data.
    """
    # If no partition column is specified, return the query as is.
    if len(partition_columns) == 0 or partition_columns[0] == "":
        log("NO partition column specified. Returning query as is")
        return query

    partition_column = partition_columns[0]

    # Check if the table already exists in BigQuery.
    table = bd.Table(dataset_id, table_id)

    # If it doesn't, return the query as is, so we can fetch the whole table.
    if not table.table_exists("staging"):
        log("NO tables was found. Returning query as is")
        return query

    blobs = get_storage_blobs(dataset_id, table_id)

    # extract only partitioned folders
    storage_partitions_dict = parser_blobs_to_partition_dict(blobs)
    # get last partition date
    last_partition_date = extract_last_partition_date(
        storage_partitions_dict, date_format
    )

    if lower_bound_date:
        last_date = min(lower_bound_date, last_partition_date)
    else:
        last_date = last_partition_date

    # Using the last partition date, get the partitioned query.
    # `aux_name` must be unique and start with a letter, for better compatibility with
    # multiple DBMSs.
    aux_name = f"a{uuid4().hex}"

    log(
        f"Partitioned DETECTED: {partition_column}, retuning a NEW QUERY "
        "with partitioned columns and filters"
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


@task
def parse_comma_separated_string_to_list(text: str) -> List[str]:
    """
    Parses a comma separated string to a list.

    Args:
        text: The text to parse.

    Returns:
        A list of strings.
    """
    if text is None or text == "":
        return []
    # Remove extras.
    text = text.replace("\n", "")
    text = text.replace("\r", "")
    text = text.replace("\t", "")
    while text.find(",,") != -1:
        text = text.replace(",,", ",")
    while text.endswith(","):
        text = text[:-1]
    result = [x.strip() for x in text.split(",")]
    result = [item for item in result if item != "" and item is not None]
    return result


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
    nout=2,
)
def dump_batches_to_csv(
    database: Database,
    batch_size: int,
    prepath: Union[str, Path],
    partition_columns: List[str] = None,
    wait=None,  # pylint: disable=unused-argument
) -> Path:
    """
    Dumps batches of data to CSV.
    """
    # Get columns
    columns = database.get_columns()
    log(f"Got columns: {columns}")

    new_query_cols = build_query_new_columns(table_columns=columns)
    log("New query columns without accents:")
    log(f"{new_query_cols}")

    prepath = Path(prepath)
    log(f"Got prepath: {prepath}")

    if len(partition_columns) == 0 or partition_columns[0] == "":
        partition_column = None
    else:
        partition_column = partition_columns[0]

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
        old_columns = dataframe.columns.tolist()
        dataframe.columns = remove_columns_accents(dataframe)
        new_columns_dict = dict(zip(old_columns, dataframe.columns.tolist()))
        if idx == 0:
            log(f"New columns without accents: {new_columns_dict}")

        dataframe = clean_dataframe(dataframe)
        # Write to CSV
        if not partition_column:
            dataframe_to_csv(dataframe, prepath / f"{eventid}-{idx}.csv")
        else:
            dataframe, date_partition_columns = parse_date_columns(
                dataframe, new_columns_dict[partition_column]
            )

            partitions = date_partition_columns + [
                new_columns_dict[col] for col in partition_columns[1:]
            ]
            to_partitions(
                dataframe,
                partitions,
                prepath,
            )
        # Get next batch
        batch = database.fetch_batch(batch_size)
        idx += 1

    log(f"Dumped {idx} batches with size {len(batch)}, total of {idx*batch_size}")

    return prepath, idx
