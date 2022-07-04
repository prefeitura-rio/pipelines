# -*- coding: utf-8 -*-
"""
Flows for comando
"""

from datetime import timedelta

from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.rj_cor.comando.eventos.tasks import (
    get_and_save_date_redis,
    download,
    salvar_dados,
)

from pipelines.rj_cor.comando.eventos.constants import (
    constants as comando_constants,
)

# from pipelines.rj_cor.comando.schedules import every_two_weeks
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.dump_db.constants import constants as dump_db_constants
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.tasks import (
    get_current_flow_labels,
    create_table_and_upload_to_gcs,
)

with Flow(
    "COR: Comando - Eventos e Atividades do Evento",
    code_owners=[
        "paty",
    ],
) as rj_cor_comando_flow:

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

    dataset_id = comando_constants.DATASET_ID.value
    table_id_eventos = comando_constants.TABLE_ID_EVENTOS.value
    table_id_atividades_eventos = comando_constants.TABLE_ID_ATIVIDADES_EVENTOS.value
    dump_mode = "append"

    date_interval = get_and_save_date_redis(
        dataset_id=dataset_id, table_id=table_id_eventos, mode="dev"
    )
    eventos, atividade_eventos = download(date_interval)
    eventos_path = salvar_dados(dfr=eventos, current_time=date_interval["fim"])
    # atividade_eventos_path = salvar_dados(
    #    dfr=atividade_eventos,
    #    current_time=date_interval['fim']
    # )

    create_table_and_upload_to_gcs(
        data_path=eventos_path,
        dataset_id=dataset_id,
        table_id=table_id_eventos,
        dump_mode="append",
        wait=eventos_path,
    )

    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id_eventos,
                "mode": materialization_mode,
                "materialize_to_datario": materialize_to_datario,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id_eventos}",
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
                    "table_id": table_id_eventos,
                    "maximum_bytes_processed": maximum_bytes_processed,
                },
                labels=[
                    "datario",
                ],
                run_name=f"Dump to GCS {dataset_id}.{table_id_eventos}",
            )
            dump_to_gcs_flow.set_upstream(wait_for_materialization)

            wait_for_dump_to_gcs = wait_for_flow_run(
                dump_to_gcs_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )

rj_cor_comando_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_cor_comando_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow.schedule = every_two_weeks
