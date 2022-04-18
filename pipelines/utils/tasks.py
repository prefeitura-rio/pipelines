# -*- coding: utf-8 -*-
"""
Helper tasks that could fit any pipeline.
"""
# pylint: disable=unused-argument

from datetime import timedelta
from os import walk
from os.path import join
from pathlib import Path
from typing import List, Union, Any
from uuid import uuid4

import basedosdados as bd
import pandas as pd
import pendulum
import prefect
from prefect import task
from prefect.backend import FlowRunView
from prefect.client import Client

from pipelines.constants import constants
from pipelines.utils.utils import get_username_and_password_from_secret, log

##################
#
# Utilities for flow management
#
##################


@prefect.task(checkpoint=False)
def log_task(msg: Any, level: str = "info", wait=None):
    """
    Logs a message to prefect's logger.
    """
    log(msg, level)


@prefect.task(checkpoint=False)
def get_now_time():
    """
    Returns the HH:MM.
    """
    now = pendulum.now(pendulum.timezone("America/Sao_Paulo"))

    return f"{now.hour}:{f'0{now.minute}' if len(str(now.minute))==1 else now.minute}"


@task
def get_current_flow_labels() -> List[str]:
    """
    Get the labels of the current flow.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    flow_run_view = FlowRunView.from_flow_run_id(flow_run_id)
    return flow_run_view.labels


@task
def greater_than(value, compare_to) -> bool:
    """
    Returns True if value is greater than compare_to.
    """
    return value > compare_to


@task
def rename_current_flow_run_now_time(prefix: str, now_time=None, wait=None) -> None:
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()
    return client.set_flow_run_name(flow_run_id, f"{prefix}{now_time}")


@task
def rename_current_flow_run_dataset_table(
    prefix: str, dataset_id, table_id, wait=None
) -> None:
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()
    return client.set_flow_run_name(flow_run_id, f"{prefix}{dataset_id}.{table_id}")


##################
#
# Hashicorp Vault
#
##################


@task(checkpoint=False, nout=2)
def get_user_and_password(secret_path: str, wait=None):
    """
    Returns the user and password for the given secret path.
    """
    log(f"Getting user and password for secret path: {secret_path}")
    return get_username_and_password_from_secret(secret_path)


###############
#
# Upload to GCS
#
###############
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def dump_header_to_csv(
    data_path: Union[str, Path],
    wait=None,  # pylint: disable=unused-argument
):
    """
    Writes a header to a CSV file.
    """
    # Remove filename from path
    path = Path(data_path)
    if not path.is_dir():
        path = path.parent
    # Grab first CSV file found
    found: bool = False
    file: str = None
    for subdir, _, filenames in walk(str(path)):
        for fname in filenames:
            if fname.endswith(".csv"):
                file = join(subdir, fname)
                log(f"Found CSV file: {file}")
                found = True
                break
        if found:
            break

    save_header_path = f"data/{uuid4()}"
    # discover if it's a partitioned table
    if partition_folders := [folder for folder in file.split("/") if "=" in folder]:
        partition_path = "/".join(partition_folders)
        save_header_file_path = Path(f"{save_header_path}/{partition_path}/header.csv")
        log(f"Found partition path: {save_header_file_path}")

    else:
        save_header_file_path = Path(f"{save_header_path}/header.csv")
        log(f"Do not found partition path: {save_header_file_path}")

    # Create directory if it doesn't exist
    save_header_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Read just first row
    dataframe = pd.read_csv(file, nrows=1)

    # Write dataframe to CSV
    dataframe.to_csv(save_header_file_path, index=False, encoding="utf-8")
    log(f"Wrote header CSV: {save_header_file_path}")

    return save_header_path


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
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

    # prod datasets is public if the project is datario. staging are private im both projects
    dataset_is_public = tb.client["bigquery_prod"].project == "datario"

    # full dump
    if dump_type == "append":
        if tb.table_exists(mode="staging"):
            log(
                f"Mode append: Table {st.bucket_name}.{dataset_id}.{table_id} already exists"
            )
        else:
            tb.create(
                path=path,
                if_storage_data_exists="replace",
                if_table_config_exists="replace",
                if_table_exists="replace",
                location="southamerica-east1",
                dataset_is_public=dataset_is_public,
            )
            log(
                "Mode append: Sucessfully created a new table "
                f"{st.bucket_name}.{dataset_id}.{table_id}"
            )  # pylint: disable=C0301

            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
            log(
                "Mode append: Sucessfully remove header data from "
                f"{st.bucket_name}.{dataset_id}.{table_id}"
            )  # pylint: disable=C0301
    elif dump_type == "overwrite":
        if tb.table_exists(mode="staging"):
            log(
                f"Mode overwrite: Table {st.bucket_name}.{dataset_id}.{table_id} "
                "already exists, DELETING OLD DATA!"
            )  # pylint: disable=C0301
            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )

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
            f"Mode overwrite: Sucessfully remove header data from "
            f"{st.bucket_name}.{dataset_id}.{table_id}"
        )  # pylint: disable=C0301


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def upload_to_gcs(
    path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    partitions=None,
    dump_type: str = "append",
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """
    Uploads a bunch of CSVs using BD+
    """
    # pylint: disable=C0103
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    # st = bd.Storage(dataset_id=dataset_id, table_id=table_id)

    if dump_type == "overwrite":
        st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
        st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)

    if tb.table_exists(mode="staging"):
        # the name of the files need to be the same or the data doesn't get overwritten
        tb.append(filepath=path, if_exists="replace", partitions=partitions)

        log(
            f"Successfully uploaded {path} to {tb.bucket_name}.staging.{dataset_id}.{table_id}"
        )

    else:
        # pylint: disable=C0301
        log(
            "Table does not exist in STAGING, need to create it in local first.\n"
            "Create and publish the table in BigQuery first."
        )


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def check_table_exists(
    dataset_id: str,
    table_id: str,
    wait=None,  # pylint: disable=unused-argument
) -> bool:
    """
    Check if table exists in staging on GCP
    """
    # pylint: disable=C0103
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    exists = tb.table_exists(mode="staging")
    log(f"Table {dataset_id}.{table_id} exists in staging: {exists}")
    return exists
