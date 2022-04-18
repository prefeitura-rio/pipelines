# -*- coding: utf-8 -*-
"""
Flows for precipitacao_alertario
"""
from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.precipitacao_alertario.tasks import (
    download,
    tratar_dados,
    salvar_dados,
)
from pipelines.rj_cor.meteorologia.precipitacao_alertario.schedules import (
    minute_schedule,
)
from pipelines.utils.tasks import create_table_and_upload_to_gcs

with Flow(
    "COR: Meteorologia - Precipitacao ALERTARIO"
) as cor_meteorologia_precipitacao_alertario:

    DATASET_ID = "meio_ambiente_clima"
    TABLE_ID = "precipitacao_alertario"
    DUMP_TYPE = "append"

    filename, current_time = download()
    dados = tratar_dados(filename=filename)
    path, partitions = salvar_dados(dados=dados, current_time=current_time)

    # Create table in BigQuery
    create_table_and_upload_to_gcs(
        data_path=path,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dump_type=DUMP_TYPE,
        partitions=partitions,
        wait=path,
    )

# para rodar na cloud
cor_meteorologia_precipitacao_alertario.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_precipitacao_alertario.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
cor_meteorologia_precipitacao_alertario.schedule = minute_schedule
