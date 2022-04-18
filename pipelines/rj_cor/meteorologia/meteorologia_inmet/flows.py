# -*- coding: utf-8 -*-
"""
Flows for meteorologia_inmet
"""
import pendulum

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.meteorologia_inmet.tasks import (
    slice_data,
    download,
    tratar_dados,
    salvar_dados,
)
from pipelines.rj_cor.meteorologia.meteorologia_inmet.schedules import hour_schedule
from pipelines.utils.tasks import create_table_and_upload_to_gcs

with Flow(
    "COR: Meteorologia - Meteorologia INMET"
) as cor_meteorologia_meteorologia_inmet:
    CURRENT_TIME = pendulum.now("UTC")  # segundo o manual é UTC

    DATASET_ID = "meio_ambiente_clima"
    TABLE_ID = "meteorologia_inmet"
    DUMP_TYPE = "append"

    data, hora = slice_data(current_time=CURRENT_TIME)

    dados = download(data=data)
    dados, partitions = tratar_dados(dados=dados, hora=hora)
    path = salvar_dados(dados=dados, partitions=partitions)

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
cor_meteorologia_meteorologia_inmet.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_meteorologia_inmet.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_meteorologia_inmet.schedule = hour_schedule
