# -*- coding: utf-8 -*-
"""
Flows para pipeline de dados de nível de lâmina de água em via.
"""
# pylint: disable=C0327, C0103

from datetime import timedelta

from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.rj_rioaguas.saneamento_drenagem.nivel_lamina_agua.tasks import (
    download_file,
    tratar_dados,
    salvar_dados,
)
from pipelines.rj_rioaguas.saneamento_drenagem.nivel_lamina_agua.schedules import (
    MINUTE_SCHEDULE,
)
from pipelines.utils.dump_db.constants import (
    constants as dump_db_constants,
)
from pipelines.utils.dump_to_gcs.constants import (
    constants as dump_to_gcs_constants,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
)

with Flow(
    "RIOAGUAS: Lamina de água em via",
    code_owners=["JP"],
) as rioaguas_lamina_agua:
    # Parâmetros para a Materialização
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    materialize_to_datario = Parameter(
        "materialize_to_datario", default=False, required=False
    )
    materialization_mode = Parameter("mode", default="dev", required=False)

    # Parâmetros para salvar dados no GCS
    dataset_id = "saneamento_drenagem"
    table_id = "nivel_lamina_agua_via"
    dump_mode = "append"

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    dump_to_gcs = Parameter("dump_to_gcs", default=False, required=False)

    maximum_bytes_processed = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )

    # Tasks
    dados = download_file()
    dados_tratados = tratar_dados(dados, dataset_id, table_id)
    save_path = salvar_dados(dados_tratados)

    # Create table in BigQuery
    upload_table = create_table_and_upload_to_gcs(
        data_path=save_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        wait=save_path,
    )

    # Trigger DBT flow run
    with case(materialize_after_dump, True):
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

        materialization_flow.set_upstream(upload_table)

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

rioaguas_lamina_agua.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rioaguas_lamina_agua.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_RIOAGUAS_AGENT_LABEL.value,
    ],
)
# rioaguas_lamina_agua.schedule = MINUTE_SCHEDULE
rioaguas_lamina_agua.schedule = None
