# -*- coding: utf-8 -*-
"""
Helper tasks that could fit any pipeline.
"""
# pylint: disable=unused-argument, R0913

from datetime import timedelta

from pathlib import Path
from typing import List, Union, Any

import basedosdados as bd
import pendulum
import prefect
from prefect import task
from prefect.backend import FlowRunView
from prefect.client import Client

from pipelines.constants import constants
from pipelines.utils.utils import (
    get_username_and_password_from_secret,
    log,
    dump_header_to_file,
)

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


@prefect.task(checkpoint=False)
def get_now_date():
    """
    Returns the YYYY-MM-DD.
    """
    now = pendulum.now(pendulum.timezone("America/Sao_Paulo"))

    return now.strftime("YYYY-MM-DD")


@task
def get_current_flow_labels() -> List[str]:
    """
    Get the labels of the current flow.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    flow_run_view = FlowRunView.from_flow_run_id(flow_run_id)
    return flow_run_view.labels


@task
def get_current_flow_mode(labels: List[str]) -> str:
    """
    Get the mode (prod/dev/staging) of the current flow.
    """
    if labels[0].endswith("-dev"):
        return "dev"
    if labels[0].endswith("-staging"):
        return "staging"
    return "prod"


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
def create_table_and_upload_to_gcs(
    data_path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """
    Create table using BD+ and upload to GCS.
    """
    # pylint: disable=C0103
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    table_staging = f"{tb.table_full_name['staging']}"
    # pylint: disable=C0103
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
    storage_path_link = (
        f"https://console.cloud.google.com/storage/browser/{st.bucket_name}"
        f"/staging/{dataset_id}/{table_id}"
    )

    # prod datasets is public if the project is datario. staging are private im both projects
    dataset_is_public = tb.client["bigquery_prod"].project == "datario"

    #####################################
    #
    # MANAGEMENT OF TABLE CREATION
    #
    #####################################
    log("STARTING TABLE CREATION MANAGEMENT")
    if dump_mode == "append":
        if tb.table_exists(mode="staging"):
            log(
                f"MODE APPEND: Table ALREADY EXISTS:"
                f"\n{table_staging}"
                f"\n{storage_path_link}"
            )
        else:
            # the header is needed to create a table when dosen't exist
            log("MODE APPEND: Table DOSEN'T EXISTS\nStart to CREATE HEADER file")
            header_path = dump_header_to_file(data_path=data_path)
            log("MODE APPEND: Created HEADER file:\n" f"{header_path}")

            tb.create(
                path=header_path,
                if_storage_data_exists="replace",
                if_table_config_exists="replace",
                if_table_exists="replace",
                dataset_is_public=dataset_is_public,
            )

            log(
                "MODE APPEND: Sucessfully CREATED A NEW TABLE:\n"
                f"{table_staging}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301

            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
            log(
                "MODE APPEND: Sucessfully REMOVED HEADER DATA from Storage:\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301
    elif dump_mode == "overwrite":
        if tb.table_exists(mode="staging"):
            log(
                "MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301
            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
            log(
                "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301
            tb.delete(mode="all")
            log(
                "MODE OVERWRITE: Sucessfully DELETED TABLE:\n"
                f"{table_staging}\n"
                f"{tb.table_full_name['prod']}"
            )  # pylint: disable=C0301

        # the header is needed to create a table when dosen't exist
        # in overwrite mode the header is always created
        log("MODE OVERWRITE: Table DOSEN'T EXISTS\nStart to CREATE HEADER file")
        header_path = dump_header_to_file(data_path=data_path)
        log("MODE OVERWRITE: Created HEADER file:\n" f"{header_path}")

        tb.create(
            path=header_path,
            if_storage_data_exists="replace",
            if_table_config_exists="replace",
            if_table_exists="replace",
            dataset_is_public=dataset_is_public,
        )

        log(
            "MODE OVERWRITE: Sucessfully CREATED TABLE\n"
            f"{table_staging}\n"
            f"{storage_path_link}"
        )

        st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
        log(
            f"MODE OVERWRITE: Sucessfully REMOVED HEADER DATA from Storage\n:"
            f"{storage_path}\n"
            f"{storage_path_link}"
        )  # pylint: disable=C0301

    #####################################
    #
    # Uploads a bunch of files using BD+
    #
    #####################################

    log("STARTING UPLOAD TO GCS")
    if tb.table_exists(mode="staging"):
        # the name of the files need to be the same or the data doesn't get overwritten
        tb.append(filepath=data_path, if_exists="replace")

        log(
            f"STEP UPLOAD: Successfully uploaded {data_path} to Storage:\n"
            f"{storage_path}\n"
            f"{storage_path_link}"
        )
    else:
        # pylint: disable=C0301
        log("STEP UPLOAD: Table does not exist in STAGING, need to create first")


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
