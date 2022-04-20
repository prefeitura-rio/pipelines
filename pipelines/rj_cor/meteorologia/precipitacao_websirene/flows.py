# -*- coding: utf-8 -*-
"""
Flows for precipitacao_alertario
"""
from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.precipitacao_websirene.tasks import (
    download_tratar_dados,
    salvar_dados,
)
from pipelines.rj_cor.meteorologia.precipitacao_alertario.schedules import (
    minute_schedule,
)
from pipelines.utils.tasks import create_table_and_upload_to_gcs

with Flow(
    "COR: Meteorologia - Precipitacao WEBSIRENE"
) as cor_meteorologia_precipitacao_websirene:

    DATASET_ID = "meio_ambiente_clima"
    TABLE_ID = "precipitacao_websirene"
    DUMP_TYPE = "append"

    dfr = download_tratar_dados()
    PATH, partitions = salvar_dados(dfr=dfr)

    # Create table in BigQuery
    create_table_and_upload_to_gcs(
        data_path=PATH,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dump_type=DUMP_TYPE,
        partitions=partitions,
        wait=PATH,
    )

# para rodar na cloud
cor_meteorologia_precipitacao_websirene.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_precipitacao_websirene.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
cor_meteorologia_precipitacao_websirene.schedule = minute_schedule
