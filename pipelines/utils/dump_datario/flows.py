# -*- coding: utf-8 -*-
# pylint: disable=invalid-name, C0103, E1120
"""
Database dumping flows.
"""

from uuid import uuid4

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.dump_datario.tasks import (
    get_datario_geodataframe,
    transform_geodataframe,
)

with Flow(
    name=utils_constants.FLOW_DUMP_DATARIO_NAME.value,
    code_owners=[
        "diego",
    ],
) as dump_datario_flow:
    #####################################
    #
    # Parameters
    #
    #####################################

    # Datario
    url = Parameter("url")
    geometry_column = Parameter("geometry_column", default="geometry", required=False)
    convert_to_crs_4326 = Parameter(
        "convert_to_crs_4326", default=False, required=False
    )
    geometry_3d_to_2d = Parameter("geometry_3d_to_2d", default=False, required=False)
    batch_size = Parameter("batch_size", default=100, required=False)

    # BigQuery parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    # overwrite or append
    dump_mode = Parameter("dump_mode", default="overwrite")
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
    biglake_table = Parameter("biglake_table", default=False, required=False)
    #####################################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    #####################################
    #
    # Tasks section #1 - Create table
    #
    #####################################

    file_path = get_datario_geodataframe(
        url=url,
        path=f"data/{uuid4()}/",
        wait=rename_flow_run,
    )
    file_path.set_upstream(rename_flow_run)

    datario_path = transform_geodataframe(
        file_path=file_path,
        batch_size=batch_size,
        geometry_column=geometry_column,
        convert_to_crs_4326=convert_to_crs_4326,
        geometry_3d_to_2d=geometry_3d_to_2d,
        wait=file_path,
    )
    datario_path.set_upstream(file_path)

    CREATE_TABLE_AND_UPLOAD_TO_GCS_DONE = create_table_and_upload_to_gcs(
        data_path=datario_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
        wait=datario_path,
    )
    CREATE_TABLE_AND_UPLOAD_TO_GCS_DONE.set_upstream(datario_path)

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
                "materialize_to_datario": materialize_to_datario,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id}",
        )
        materialization_flow.set_upstream(CREATE_TABLE_AND_UPLOAD_TO_GCS_DONE)

        wait_for_materialization = wait_for_flow_run(
            materialization_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
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

dump_datario_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_datario_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
