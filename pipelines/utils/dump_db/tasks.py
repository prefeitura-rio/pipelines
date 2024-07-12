# -*- coding: utf-8 -*-
# pylint: disable=R0914
"""
General purpose tasks for dumping database data.
"""
from datetime import datetime, timedelta
from queue import Empty, Queue
from pathlib import Path
import re
from threading import Event, Thread
from time import sleep, time
from typing import Dict, List, Union
from uuid import uuid4

import basedosdados as bd
import pandas as pd
from prefect import task

from pipelines.utils.dump_db.db import (
    Database,
    MySql,
    Oracle,
    SqlServer,
)
from pipelines.utils.dump_db.utils import (
    extract_last_partition_date,
    build_query_new_columns,
)
from pipelines.utils.elasticsearch_metrics.utils import (
    format_document,
    index_document,
)
from pipelines.utils.utils import (
    batch_to_dataframe,
    dataframe_to_csv,
    dataframe_to_parquet,
    delete_blobs_list,
    dump_header_to_file,
    list_blobs_with_prefix,
    parse_date_columns,
    clean_dataframe,
    to_partitions,
    parser_blobs_to_partition_dict,
    remove_tabs_from_query,
    get_storage_blobs,
    remove_columns_accents,
)
from pipelines.constants import constants
from pipelines.utils.utils import log, log_mod

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
    flow_name: str = None,
    labels: List[str] = None,
    dataset_id: str = None,
    table_id: str = None,
) -> None:
    """
    Executes a query on the database.

    Args:
        database: The database object.
        query: The query to execute.
    """
    start_time = time()
    log(f"Query parsed: {query}")
    query = remove_tabs_from_query(query)
    log(f"Executing query line: {query}")
    database.execute_query(query)
    time_elapsed = time() - start_time
    doc = format_document(
        flow_name=flow_name,
        labels=labels,
        event_type="db_execute",
        dataset_id=dataset_id,
        table_id=table_id,
        metrics={"db_execute": time_elapsed},
    )
    index_document(doc)


@task(
    checkpoint=False,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def database_fetch(
    database: Database,
    batch_size: str,
    wait=None,  # pylint: disable=unused-argument
    flow_name: str = None,
    labels: List[str] = None,
    dataset_id: str = None,
    table_id: str = None,
):
    """
    Fetches the results of a query on the database.
    """
    start_time = time()
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
    time_elapsed = time() - start_time
    doc = format_document(
        flow_name=flow_name,
        labels=labels,
        event_type="db_fetch",
        dataset_id=dataset_id,
        table_id=table_id,
        metrics={"db_fetch": time_elapsed},
    )
    index_document(doc)


@task(
    checkpoint=False,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def format_partitioned_query(
    query: str,
    dataset_id: str,
    table_id: str,
    database_type: str,
    partition_columns: List[str] = None,
    lower_bound_date: str = None,
    date_format: str = None,
    wait=None,  # pylint: disable=unused-argument
):
    """
    Formats a query for fetching partitioned data.
    """
    # If no partition column is specified, return the query as is.
    if not partition_columns or partition_columns[0] == "":
        log("NO partition column specified. Returning query as is")
        return query

    partition_column = partition_columns[0]

    # Check if the table already exists in BigQuery.
    table = bd.Table(dataset_id, table_id)

    # If it doesn't, return the query as is, so we can fetch the whole table.
    if not table.table_exists(mode="staging"):
        log("NO tables was found. Returning query as is")
        return query

    blobs = get_storage_blobs(dataset_id, table_id)

    # extract only partitioned folders
    storage_partitions_dict = parser_blobs_to_partition_dict(blobs)
    # get last partition date
    last_partition_date = extract_last_partition_date(
        storage_partitions_dict, date_format
    )

    if lower_bound_date == "current_year":
        lower_bound_date = datetime.now().replace(month=1, day=1).strftime("%Y-%m-%d")
        log(f"Using lower_bound_date current_year: {lower_bound_date}")
    elif lower_bound_date == "current_month":
        lower_bound_date = datetime.now().replace(day=1).strftime("%Y-%m-%d")
        log(f"Using lower_bound_date current_month: {lower_bound_date}")
    elif lower_bound_date == "current_day":
        lower_bound_date = datetime.now().strftime("%Y-%m-%d")
        log(f"Using lower_bound_date current_day: {lower_bound_date}")

    if lower_bound_date:
        last_date = min(str(lower_bound_date), str(last_partition_date))
        log(f"Using lower_bound_date: {last_date}")

    else:
        last_date = str(last_partition_date)
        log(f"Using last_date from storage: {last_date}")

    # Using the last partition date, get the partitioned query.
    # `aux_name` must be unique and start with a letter, for better compatibility with
    # multiple DBMSs.
    aux_name = f"a{uuid4().hex}"[:8]

    log(
        f"Partitioned DETECTED: {partition_column}, retuning a NEW QUERY "
        "with partitioned columns and filters"
    )
    if database_type == "oracle":
        oracle_date_format = "YYYY-MM-DD" if date_format == "%Y-%m-%d" else date_format

        return f"""
        with {aux_name} as ({query})
        select * from {aux_name}
        where {partition_column} >= TO_DATE('{last_date}', '{oracle_date_format}')
        """

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
    if text is None or not text:
        return []
    # Remove extras.
    text = text.replace("\n", "")
    text = text.replace("\r", "")
    text = text.replace("\t", "")
    while ",," in text:
        text = text.replace(",,", ",")
    while text.endswith(","):
        text = text[:-1]
    result = [x.strip() for x in text.split(",")]
    result = [item for item in result if item != "" and item is not None]
    return result


@task(max_retries=3, retry_delay=timedelta(seconds=300))
def dump_upload_batch(
    database: Database,
    batch_size: int,
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    partition_columns: List[str] = None,
    batch_data_type: str = "csv",
    biglake_table: bool = True,
    log_number_of_batches: int = 100,
):
    """
    This task will dump and upload batches of data, sequentially.
    """
    # Log BD version
    bd_version = bd.__version__
    log(f"Using basedosdados@{bd_version}")

    # Keep track of cleared stuff
    prepath = f"data/{uuid4()}/"
    cleared_partitions = set()
    cleared_table = False

    # Get data columns
    columns = database.get_columns()
    log(f"Got columns: {columns}")

    new_query_cols = build_query_new_columns(table_columns=columns)
    log(f"New query columns without accents: {new_query_cols}")

    prepath = Path(prepath)
    log(f"Got prepath: {prepath}")

    if not partition_columns or partition_columns[0] == "":
        partition_column = None
    else:
        partition_column = partition_columns[0]

    if not partition_column:
        log("NO partition column specified! Writing unique files")
    else:
        log(f"Partition column: {partition_column} FOUND!! Write to partitioned files")

    # Now loop until we have no more data.
    batch = database.fetch_batch(batch_size)
    idx = 0
    while len(batch) > 0:
        # Log progress each 100 batches.
        log_mod(
            msg=f"Dumping batch {idx} with size {len(batch)}",
            index=idx,
            mod=log_number_of_batches,
        )

        # Dump batch to file.
        dataframe = batch_to_dataframe(batch, columns)
        old_columns = dataframe.columns.tolist()
        dataframe.columns = remove_columns_accents(dataframe)
        new_columns_dict = dict(zip(old_columns, dataframe.columns.tolist()))
        dataframe = clean_dataframe(dataframe)
        saved_files = []
        if partition_column:
            dataframe, date_partition_columns = parse_date_columns(
                dataframe, new_columns_dict[partition_column]
            )
            partitions = date_partition_columns + [
                new_columns_dict[col] for col in partition_columns[1:]
            ]
            saved_files = to_partitions(
                data=dataframe,
                partition_columns=partitions,
                savepath=prepath,
                data_type=batch_data_type,
                suffix=f"{datetime.now().strftime('%Y%m%d-%H%M%S')}",
            )
        elif batch_data_type == "csv":
            fname = prepath / f"{uuid4()}.csv"
            dataframe_to_csv(dataframe, fname)
            saved_files = [fname]
        elif batch_data_type == "parquet":
            fname = prepath / f"{uuid4()}.parquet"
            dataframe_to_parquet(dataframe, fname)
            saved_files = [fname]
        else:
            raise ValueError(f"Unknown data type: {batch_data_type}")

        # Log progress each 100 batches.

        log_mod(
            msg=f"Batch generated {len(saved_files)} files. Will now upload.",
            index=idx,
            mod=log_number_of_batches,
        )

        # Upload files.
        tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
        table_staging = f"{tb.table_full_name['staging']}"
        st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
        storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
        storage_path_link = (
            f"https://console.cloud.google.com/storage/browser/{st.bucket_name}"
            f"/staging/{dataset_id}/{table_id}"
        )
        dataset_is_public = tb.client["bigquery_prod"].project == "datario"
        # If we have a partition column
        if partition_column:
            # Extract the partition from the filenames
            partitions = []
            for saved_file in saved_files:
                # Remove the prepath and filename. This is the partition.
                partition = str(saved_file).replace(str(prepath), "")
                partition = partition.replace(saved_file.name, "")
                # Strip slashes from beginning and end.
                partition = partition.strip("/")
                # Add to list.
                partitions.append(partition)
            # Remove duplicates.
            partitions = list(set(partitions))
            log_mod(
                msg=f"Got partitions: {partitions}",
                index=idx,
                mod=log_number_of_batches,
            )
            # Loop through partitions and delete files from GCS.
            blobs_to_delete = []
            for partition in partitions:
                if partition not in cleared_partitions:
                    blobs = list_blobs_with_prefix(
                        bucket_name=st.bucket_name,
                        prefix=f"staging/{dataset_id}/{table_id}/{partition}",
                    )
                    blobs_to_delete.extend(blobs)
                cleared_partitions.add(partition)
            if blobs_to_delete:
                delete_blobs_list(bucket_name=st.bucket_name, blobs=blobs_to_delete)
                log_mod(
                    msg=f"Deleted {len(blobs_to_delete)} blobs from GCS: {blobs_to_delete}",
                    index=idx,
                    mod=log_number_of_batches,
                )
        if dump_mode == "append":
            if tb.table_exists(mode="staging"):
                log_mod(
                    msg=(
                        "MODE APPEND: Table ALREADY EXISTS:"
                        + f"\n{table_staging}"
                        + f"\n{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )
            else:
                # the header is needed to create a table when dosen't exist
                log_mod(
                    msg="MODE APPEND: Table DOESN'T EXISTS\nStart to CREATE HEADER file",
                    index=idx,
                    mod=log_number_of_batches,
                )
                header_path = dump_header_to_file(data_path=saved_files[0])
                log_mod(
                    msg="MODE APPEND: Created HEADER file:\n" f"{header_path}",
                    index=idx,
                    mod=log_number_of_batches,
                )

                tb.create(
                    path=header_path,
                    if_storage_data_exists="replace",
                    if_table_exists="replace",
                    biglake_table=biglake_table,
                    dataset_is_public=dataset_is_public,
                )

                log_mod(
                    msg=(
                        "MODE APPEND: Sucessfully CREATED A NEW TABLE:\n"
                        + f"{table_staging}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301

                if not cleared_table:
                    st.delete_table(
                        mode="staging",
                        bucket_name=st.bucket_name,
                        not_found_ok=True,
                    )
                    log_mod(
                        msg=(
                            "MODE APPEND: Sucessfully REMOVED HEADER DATA from Storage:\n"
                            + f"{storage_path}\n"
                            + f"{storage_path_link}"
                        ),
                        index=idx,
                        mod=log_number_of_batches,
                    )  # pylint: disable=C0301
                    cleared_table = True
        elif dump_mode == "overwrite":
            if tb.table_exists(mode="staging") and not cleared_table:
                log_mod(
                    msg=(
                        "MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301
                st.delete_table(
                    mode="staging", bucket_name=st.bucket_name, not_found_ok=True
                )
                log_mod(
                    msg=(
                        "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301
                # delete only staging table and let DBT overwrite the prod table
                tb.delete(mode="staging")
                log_mod(
                    msg=(
                        "MODE OVERWRITE: Sucessfully DELETED TABLE:\n"
                        + f"{table_staging}\n"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301

            if not cleared_table:
                # the header is needed to create a table when dosen't exist
                # in overwrite mode the header is always created
                st.delete_table(
                    mode="staging", bucket_name=st.bucket_name, not_found_ok=True
                )
                log_mod(
                    msg=(
                        "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301

                log_mod(
                    msg="MODE OVERWRITE: Table DOSEN'T EXISTS\nStart to CREATE HEADER file",
                    index=idx,
                    mod=log_number_of_batches,
                )
                header_path = dump_header_to_file(data_path=saved_files[0])
                log_mod(
                    "MODE OVERWRITE: Created HEADER file:\n" f"{header_path}",
                    index=idx,
                    mod=log_number_of_batches,
                )

                tb.create(
                    path=header_path,
                    if_storage_data_exists="replace",
                    if_table_exists="replace",
                    biglake_table=biglake_table,
                    dataset_is_public=dataset_is_public,
                )

                log_mod(
                    msg=(
                        "MODE OVERWRITE: Sucessfully CREATED TABLE\n"
                        + f"{table_staging}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )

                st.delete_table(
                    mode="staging", bucket_name=st.bucket_name, not_found_ok=True
                )
                log_mod(
                    msg=(
                        f"MODE OVERWRITE: Sucessfully REMOVED HEADER DATA from Storage\n:"
                        + f"{storage_path}\n"
                        + f"{storage_path_link}"
                    ),
                    index=idx,
                    mod=log_number_of_batches,
                )  # pylint: disable=C0301
                cleared_table = True

        log_mod(
            msg="STARTING UPLOAD TO GCS",
            index=idx,
            mod=log_number_of_batches,
        )
        if tb.table_exists(mode="staging"):
            # Upload them all at once
            tb.append(filepath=prepath, if_exists="replace")
            log_mod(
                msg="STEP UPLOAD: Sucessfully uploaded all batch files to Storage",
                index=idx,
                mod=log_number_of_batches,
            )
            for saved_file in saved_files:
                # Delete the files
                saved_file.unlink()
        else:
            # pylint: disable=C0301
            log_mod(
                msg="STEP UPLOAD: Table does not exist in STAGING, need to create first",
                index=idx,
                mod=log_number_of_batches,
            )

        # Get next batch.
        batch = database.fetch_batch(batch_size)
        idx += 1

    log(
        msg=f"Successfully dumped {idx} batches with size {len(batch)}, total of {idx*batch_size}",
    )


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
    nout=2,
)
def dump_batches_to_file(  # pylint: disable=too-many-locals,too-many-statements
    database: Database,
    batch_size: int,
    prepath: Union[str, Path],
    partition_columns: List[str] = None,
    batch_data_type: str = "csv",
    wait=None,  # pylint: disable=unused-argument
    flow_name: str = None,
    labels: List[str] = None,
    dataset_id: str = None,
    table_id: str = None,
) -> Path:
    """
    Dumps batches of data to FILE.
    """
    # Get columns
    columns = database.get_columns()
    log(f"Got columns: {columns}")

    new_query_cols = build_query_new_columns(table_columns=columns)
    log("New query columns without accents:")
    log(f"{new_query_cols}")

    prepath = Path(prepath)
    log(f"Got prepath: {prepath}")

    if not partition_columns or partition_columns[0] == "":
        partition_column = None
    else:
        partition_column = partition_columns[0]

    if not partition_column:
        log("NO partition column specified! Writing unique files")
    else:
        log(f"Partition column: {partition_column} FOUND!! Write to partitioned files")

    # Initialize queues
    batches = Queue()
    dataframes = Queue()

    # Define thread functions
    def thread_batch_to_dataframe(
        batches: Queue,
        dataframes: Queue,
        done: Event,
        columns: List[str],
        flow_name: str,
        labels: List[str],
        dataset_id: str,
        table_id: str,
    ):
        while not done.is_set():
            try:
                batch = batches.get(timeout=1)
                start_time = time()
                dataframe = batch_to_dataframe(batch, columns)
                elapsed_time = time() - start_time
                dataframes.put(dataframe)
                doc = format_document(
                    flow_name=flow_name,
                    labels=labels,
                    event_type="batch_to_dataframe",
                    dataset_id=dataset_id,
                    table_id=table_id,
                    metrics={"batch_to_dataframe": elapsed_time},
                )
                index_document(doc)
                batches.task_done()
            except Empty:
                sleep(1)

    def thread_dataframe_to_csv(
        dataframes: Queue,
        done: Event,
        flow_name: str,
        labels: List[str],
        dataset_id: str,
        table_id: str,
        partition_column: str,
        partition_columns: List[str],
        prepath: Path,
        batch_data_type: str,
        eventid: str,
    ):
        while not done.is_set():
            try:
                # Get dataframe from queue
                dataframe: pd.DataFrame = dataframes.get(timeout=1)
                # Clean dataframe
                start_time = time()
                old_columns = dataframe.columns.tolist()
                dataframe.columns = remove_columns_accents(dataframe)
                new_columns_dict = dict(zip(old_columns, dataframe.columns.tolist()))
                dataframe = clean_dataframe(dataframe)
                elapsed_time = time() - start_time
                doc = format_document(
                    flow_name=flow_name,
                    labels=labels,
                    event_type="clean_dataframe",
                    dataset_id=dataset_id,
                    table_id=table_id,
                    metrics={"clean_dataframe": elapsed_time},
                )
                index_document(doc)
                # Dump dataframe to file
                start_time = time()
                if partition_column:
                    dataframe, date_partition_columns = parse_date_columns(
                        dataframe, new_columns_dict[partition_column]
                    )
                    partitions = date_partition_columns + [
                        new_columns_dict[col] for col in partition_columns[1:]
                    ]
                    to_partitions(
                        data=dataframe,
                        partition_columns=partitions,
                        savepath=prepath,
                        data_type=batch_data_type,
                    )
                elif batch_data_type == "csv":
                    dataframe_to_csv(dataframe, prepath / f"{eventid}-{uuid4()}.csv")
                elif batch_data_type == "parquet":
                    dataframe_to_parquet(
                        dataframe, prepath / f"{eventid}-{uuid4()}.parquet"
                    )
                elapsed_time = time() - start_time
                doc = format_document(
                    flow_name=flow_name,
                    labels=labels,
                    event_type=f"batch_to_{batch_data_type}",
                    dataset_id=dataset_id,
                    table_id=table_id,
                    metrics={f"batch_to_{batch_data_type}": elapsed_time},
                )
                index_document(doc)
                dataframes.task_done()
            except Empty:
                sleep(1)

    # Initialize threads
    done = Event()
    eventid = datetime.now().strftime("%Y%m%d-%H%M%S")
    worker_batch_to_dataframe = Thread(
        target=thread_batch_to_dataframe,
        args=(
            batches,
            dataframes,
            done,
            columns,
            flow_name,
            labels,
            dataset_id,
            table_id,
        ),
    )
    worker_dataframe_to_csv = Thread(
        target=thread_dataframe_to_csv,
        args=(
            dataframes,
            done,
            flow_name,
            labels,
            dataset_id,
            table_id,
            partition_column,
            partition_columns,
            prepath,
            batch_data_type,
            eventid,
        ),
    )
    worker_batch_to_dataframe.start()
    worker_dataframe_to_csv.start()

    # Dump batches
    start_fetch_batch = time()
    batch = database.fetch_batch(batch_size)
    time_fetch_batch = time() - start_fetch_batch
    doc = format_document(
        flow_name=flow_name,
        labels=labels,
        event_type="fetch_batch",
        dataset_id=dataset_id,
        table_id=table_id,
        metrics={"fetch_batch": time_fetch_batch},
    )
    index_document(doc)
    idx = 0
    while len(batch) > 0:
        if idx % 100 == 0:
            log(f"Dumping batch {idx} with size {len(batch)}")
        # Add current batch to queue
        batches.put(batch)
        # Get next batch
        start_fetch_batch = time()
        batch = database.fetch_batch(batch_size)
        time_fetch_batch = time() - start_fetch_batch
        doc = format_document(
            flow_name=flow_name,
            labels=labels,
            event_type="fetch_batch",
            dataset_id=dataset_id,
            table_id=table_id,
            metrics={"fetch_batch": time_fetch_batch},
        )
        index_document(doc)
        idx += 1

    log("Waiting for batches queue...")
    start_sleep = 1
    max_sleep = 300
    while batches.unfinished_tasks > 0:
        sleep(start_sleep)
        start_sleep = min(start_sleep * 2, max_sleep)
        log(
            f"Waiting for {batches.unfinished_tasks} batches to be parsed as dataframes..."
        )
    batches.join()
    start_sleep = 1
    log("Waiting for dataframes queue...")
    while dataframes.unfinished_tasks > 0:
        sleep(start_sleep)
        start_sleep = min(start_sleep * 2, max_sleep)
        log(f"Waiting for {dataframes.unfinished_tasks} dataframes to be dumped...")
    dataframes.join()
    done.set()

    log("Waiting for threads to finish...")
    worker_batch_to_dataframe.join()
    worker_dataframe_to_csv.join()

    log(
        f"Successfully dumped {idx} batches with size {len(batch)}, total of {idx*batch_size}"
    )

    return prepath, idx
