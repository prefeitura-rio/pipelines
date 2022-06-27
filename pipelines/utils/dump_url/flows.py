# -*- coding: utf-8 -*-
"""
Dumping data from URLs
"""
from datetime import timedelta

from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.dump_db.constants import constants as dump_db_constants
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.dump_url.tasks import download_url
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    get_current_flow_labels,
    get_user_and_password,
    greater_than,
    rename_current_flow_run_dataset_table,
    create_table_and_upload_to_gcs,
)

with Flow(
    name=utils_constants.FLOW_DUMP_URL_NAME.value,
    code_owners=[
        "diego",
        "gabriel",
    ],
) as dump_url_flow:

    #####################################
    #
    # Parameters
    #
    #####################################

    # URL parameters
    url = Parameter("url")

    # Materialization parameters
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_to_datario = Parameter(
        "materialize_to_datario", default=False, required=False
    )

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    dump_to_gcs = Parameter("dump_to_gcs", default=False, required=False)
    maximum_bytes_processed = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )

    # BigQuery parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    dump_mode = Parameter("dump_mode", default="overwrite")  # overwrite or append
    batch_data_type = Parameter("batch_data_type", default="csv")  # csv or parquet

    #####################################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    # Get current flow labels
    current_flow_labels = get_current_flow_labels()
    current_flow_labels.set_upstream(rename_flow_run)

    #####################################
    #
    # Tasks section #1 - Get data
    #
    #####################################
    # this will not conflict with other flows because it's running in a separate container
    data_path = "/tmp/dump_url/"
    data_fname = data_path + "data.csv"
    download_url_task = download_url(
        url=url,
        data_fname=data_fname,
    )
    download_url_task.set_upstream(rename_flow_run)

    #####################################
    #
    # Tasks section #2 - Create table
    #
    #####################################
    create_table_and_upload_to_gcs_task = create_table_and_upload_to_gcs(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
    )
    create_table_and_upload_to_gcs_task.set_upstream(download_url_task)

    #####################################
    #
    # Tasks section #3 - Materialize
    #
    #####################################
    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "mode": materialization_mode,
                "materialize_to_datario": materialize_to_datario,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id}",
        )

        wait_for_materialization = wait_for_flow_run(
            materialization_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )
        wait_for_materialization.max_retries = (
            dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
        )
        wait_for_materialization.retry_delay = timedelta(
            seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
        )

        with case(dump_to_gcs, True):
            # Trigger Dump to GCS flow run with project id as datario
            dump_to_gcs_flow = create_flow_run(
                flow_name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "project_id": "datario",
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "maximum_bytes_processed": maximum_bytes_processed,
                },
                labels=[
                    "datario",
                ],
                run_name=f"Dump to GCS {dataset_id}.{table_id}",
            )
            dump_to_gcs_flow.set_upstream(wait_for_materialization)

            wait_for_dump_to_gcs = wait_for_flow_run(
                dump_to_gcs_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )

dump_url_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_url_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
