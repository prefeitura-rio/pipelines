# -*- coding: utf-8 -*-
"""
Flows for precipitacao_alertario
"""
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.precipitacao_websirene.tasks import (
    download_tratar_dados,
    salvar_dados,
)
from pipelines.rj_cor.meteorologia.precipitacao_websirene.schedules import (
    MINUTE_SCHEDULE,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import create_table_and_upload_to_gcs

with Flow(
    "COR: Meteorologia - Precipitacao WEBSIRENE",
    code_owners=[
        "@PatyBC#4954",
    ],
) as cor_meteorologia_precipitacao_websirene:

    DATASET_ID = "meio_ambiente_clima"
    TABLE_ID = "taxa_precipitacao_websirene"
    DUMP_TYPE = "append"

    DFR = download_tratar_dados()
    PATH = salvar_dados(dfr=DFR)

    # Create table in BigQuery
    create_table_and_upload_to_gcs(
        data_path=PATH,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dump_type=DUMP_TYPE,
        wait=PATH,
    )

# para rodar na cloud
cor_meteorologia_precipitacao_websirene.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_precipitacao_websirene.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
cor_meteorologia_precipitacao_websirene.schedule = MINUTE_SCHEDULE
