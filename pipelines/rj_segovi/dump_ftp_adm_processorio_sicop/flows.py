# -*- coding: utf-8 -*-
"""
Dumping  data from SICOP FTP to BigQuery 
"""
# pylint: disable=E1101

from datetime import timedelta

from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.rj_segovi.dump_ftp_adm_processorio_sicop.constants import (
    constants as dump_ftp_processorio_constants,
)
from pipelines.rj_segovi.dump_ftp_adm_processorio_sicop.tasks import (
    get_ftp_client,
    get_files_to_download,
    download_files,
    parse_save_dataframe,
)
from pipelines.rj_segovi.dump_ftp_adm_processorio_sicop.schedules import (
    every_week_schedule,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
    create_table_and_upload_to_gcs,
)

from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.utils import set_default_parameters

with Flow(
    name="SEGOVI: Processo.rio-SICOP - Ingerir tabelas de FTP",
    code_owners=[
        "diego",
    ],
) as dump_ftp_sicop:
    pattern = Parameter(
        "pattern", default="ARQ2001", required=True
    )  # processo or expediente
    dataset_id = Parameter("dataset_id", default="adm_processorio_sicop", required=True)
    table_id = Parameter("table_id", default="processo", required=True)

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    dump_to_gcs = Parameter("dump_to_gcs", default=False, required=False)
    maximum_bytes_processed = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )

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

    dbt_model_secret_parameters = Parameter(
        "dbt_model_secret_parameters", default={}, required=False
    )

    #####################################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    # Get current flow labels
    current_flow_labels = get_current_flow_labels()

    client = get_ftp_client(wait=pattern)

    files_to_download, download_data = get_files_to_download(
        client=client,
        pattern=pattern,
        dataset_id=dataset_id,
        table_id=table_id,
        date_format='"%Y-%m-%d"',
    )

    with case(download_data, True):
        files_to_parse = download_files(
            client=client, files=files_to_download, save_path="/tmp/ftp/raw"
        )

        save_path = parse_save_dataframe(
            files=files_to_parse, save_path="/tmp/ftp/data", pattern=pattern
        )

        create_table_and_upload_to_gcs(
            data_path=save_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
        )

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
                    "dbt_model_secret_parameters": dbt_model_secret_parameters,
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
                dump_ftp_processorio_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
            )
            wait_for_materialization.retry_delay = timedelta(
                seconds=dump_ftp_processorio_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
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


dump_ftp_sicop.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_ftp_sicop.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SEGOVI_AGENT_LABEL.value,
    ],
)

dump_ftp_sicop_default_parameters = {
    "pattern": "processo",  # processo or expediente
    "dataset_id": "adm_processorio_sicop",
    "table_id": "processo",
}

dump_ftp_sicop = set_default_parameters(
    dump_ftp_sicop, default_parameters=dump_ftp_sicop_default_parameters
)

dump_ftp_sicop.schedule = every_week_schedule
