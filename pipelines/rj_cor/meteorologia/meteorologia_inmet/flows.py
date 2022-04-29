# -*- coding: utf-8 -*-
"""
Flows for meteorologia_inmet
"""
from prefect import case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.rj_cor.meteorologia.meteorologia_inmet.tasks import (
    get_dates,
    slice_data,
    download,
    tratar_dados,
    salvar_dados,
)
from pipelines.rj_cor.meteorologia.meteorologia_inmet.schedules import hour_schedule
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
)


with Flow(
    name="COR: Meteorologia - Meteorologia INMET",
    code_owners=[
        "@PatyBC#4954",
    ],
) as cor_meteorologia_meteorologia_inmet:

    DATASET_ID = "meio_ambiente_clima"
    TABLE_ID = "meteorologia_inmet"
    DUMP_TYPE = "append"
    MATERIALIZE_AFTER_DUMP = True
    MATERIALIZE_TO_DATARIO = False
    MATERIALIZATION_MODE = "dev"

    CURRENT_TIME, YESTERDAY = get_dates()

    data = slice_data(current_time=CURRENT_TIME)

    dados = download(data=data, yesterday=YESTERDAY)
    dados, partitions = tratar_dados(dados=dados)
    PATH = salvar_dados(dados=dados, partitions=partitions, data=data)

    # Create table in BigQuery
    UPLOAD_TABLE = create_table_and_upload_to_gcs(
        data_path=PATH,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dump_type=DUMP_TYPE,
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
cor_meteorologia_meteorologia_inmet.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_meteorologia_inmet.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_meteorologia_inmet.schedule = hour_schedule
