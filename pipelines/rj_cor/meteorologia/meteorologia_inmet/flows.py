# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1120
"""
Flows for meteorologia_inmet
"""
from datetime import timedelta

from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.rj_cor.meteorologia.meteorologia_inmet.tasks import (
    get_dates,
    # slice_data,
    download,
    tratar_dados,
    salvar_dados,
)
from pipelines.rj_cor.meteorologia.meteorologia_inmet.schedules import hour_schedule
from pipelines.utils.decorators import Flow
from pipelines.utils.dump_db.constants import constants as dump_db_constants
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
)


with Flow(
    name="COR: Meteorologia - Meteorologia INMET",
    code_owners=[
        "paty",
    ],
) as cor_meteorologia_meteorologia_inmet:

    DATASET_ID = "clima_estacao_meteorologica"
    TABLE_ID = "meteorologia_inmet_test"
    DUMP_MODE = "append"

    # data_inicio e data_fim devem ser strings no formato "YYYY-MM-DD"
    data_inicio = Parameter("data_inicio", default="", required=False)
    data_fim = Parameter("data_fim", default="", required=False)

    # Materialization parameters
    MATERIALIZE_AFTER_DUMP = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    MATERIALIZE_TO_DATARIO = Parameter(
        "materialize_to_datario", default=False, required=False
    )
    MATERIALIZATION_MODE = Parameter("mode", default="dev", required=False)

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    DUMP_TO_GCS = Parameter("dump_to_gcs", default=False, required=False)

    MAXIMUM_BYTES_PROCESSED = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )

    data_inicio_, data_fim_, backfill = get_dates(data_inicio, data_fim)
    # data = slice_data(current_time=CURRENT_TIME)
    dados = download(data_inicio_, data_fim_)
    dados = tratar_dados(dados, backfill)
    PATH = salvar_dados(dados=dados)

    # Create table in BigQuery
    UPLOAD_TABLE = create_table_and_upload_to_gcs(
        data_path=PATH,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dump_mode=DUMP_MODE,
        wait=PATH,
    )

    # Trigger DBT flow run
    with case(MATERIALIZE_AFTER_DUMP, True):
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": DATASET_ID,
                "table_id": TABLE_ID,
                "mode": MATERIALIZATION_MODE,
                "materialize_to_datario": MATERIALIZE_TO_DATARIO,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {DATASET_ID}.{TABLE_ID}",
        )

        materialization_flow.set_upstream(UPLOAD_TABLE)

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

        with case(DUMP_TO_GCS, True):
            # Trigger Dump to GCS flow run with project id as datario
            dump_to_gcs_flow = create_flow_run(
                flow_name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "project_id": "datario",
                    "dataset_id": DATASET_ID,
                    "table_id": TABLE_ID,
                    "maximum_bytes_processed": MAXIMUM_BYTES_PROCESSED,
                },
                labels=[
                    "datario",
                ],
                run_name=f"Dump to GCS {DATASET_ID}.{TABLE_ID}",
            )
            dump_to_gcs_flow.set_upstream(wait_for_materialization)

            wait_for_dump_to_gcs = wait_for_flow_run(
                dump_to_gcs_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )


# para rodar na cloud
cor_meteorologia_meteorologia_inmet.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_meteorologia_inmet.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_meteorologia_inmet.schedule = hour_schedule
