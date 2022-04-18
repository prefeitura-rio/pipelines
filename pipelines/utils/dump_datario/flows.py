# -*- coding: utf-8 -*-
"""
Database dumping flows
"""

from functools import partial
from uuid import uuid4

from prefect import Flow, Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.tasks import (
    create_bd_table,
    get_current_flow_labels,
    rename_current_flow_run,
    upload_to_gcs,
    dump_header_to_csv,
    check_table_exists,
)
from pipelines.utils.dump_datario.tasks import (
    get_datario_geodataframe,
)
from pipelines.utils.utils import notify_discord_on_failure

with Flow(
    name=utils_constants.FLOW_DUMP_DATARIO_NAME.value,
    on_failure=partial(
        notify_discord_on_failure,
        secret_path=constants.EMD_DISCORD_WEBHOOK_SECRET_PATH.value,
    ),
) as dump_datario_flow:

    #####################################
    #
    # Parameters
    #
    #####################################

    # Datario
    url = Parameter("url")

    # BigQuery parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    # overwrite or append
    dump_type = Parameter("dump_type", default="overwrite")

    # Materialization parameters
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    materialization_mode = Parameter(
        "materialization_mode", required=False, default="dev"
    )

    #####################################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run(
        prefix="Dump", dataset_id=dataset_id, table_id=table_id
    )

    #####################################
    #
    # Tasks section #1 - Create table
    #
    #####################################

    datario_path = get_datario_geodataframe(  # pylint: disable=invalid-name
        url=url, path=f"data/{uuid4()}/"
    )

    EXISTS = check_table_exists(
        dataset_id=dataset_id, table_id=table_id, wait=datario_path
    )

    # Create header and table if they don't exists
    with case(EXISTS, False):

        # Create CSV file with headers
        header_path = dump_header_to_csv(
            data_path=datario_path,
            wait=datario_path,
        )

        # Create table in BigQuery
        create_db = create_bd_table(  # pylint: disable=invalid-name
            path=header_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_type=dump_type,
            wait=header_path,
        )

        #####################################
        #
        # Tasks section #2 - Dump batches
        #
        #####################################

        # Upload to GCS
        upload_to_gcs(
            path=datario_path,
            dataset_id=dataset_id,
            table_id=table_id,
            wait=create_db,
        )

    with case(EXISTS, True):
        upload_to_gcs(
            path=datario_path,
            dataset_id=dataset_id,
            table_id=table_id,
            wait=EXISTS,
        )

    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "mode": materialization_mode,
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

dump_datario_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_datario_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
