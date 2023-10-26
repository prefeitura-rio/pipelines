# -*- coding: utf-8 -*-
"""
Flows for precipitacao_alertario
"""
from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.precipitacao_websirene.tasks import (
    download_tratar_dados,
    salvar_dados,
)
from pipelines.rj_cor.meteorologia.precipitacao_websirene.schedules import (
    MINUTE_SCHEDULE,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
)

with Flow(
    "COR: Meteorologia - Precipitacao WEBSIRENE",
    code_owners=[
        "paty",
    ],
) as cor_meteorologia_precipitacao_websirene:

    DATASET_ID = "clima_pluviometro"
    TABLE_ID = "taxa_precipitacao_websirene"
    DUMP_MODE = "append"

    # Materialization parameters
    MATERIALIZE_AFTER_DUMP = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    MATERIALIZE_TO_DATARIO = Parameter(
        "materialize_to_datario", default=False, required=False
    )
    MATERIALIZATION_MODE = Parameter("mode", default="dev", required=False)

    DFR = download_tratar_dados()
    PATH = salvar_dados(dfr=DFR)

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

# para rodar na cloud
cor_meteorologia_precipitacao_websirene.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_precipitacao_websirene.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
cor_meteorologia_precipitacao_websirene.schedule = MINUTE_SCHEDULE
