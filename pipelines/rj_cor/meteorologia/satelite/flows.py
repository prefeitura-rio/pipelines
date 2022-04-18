# -*- coding: utf-8 -*-
"""
Flows for emd
"""
import pendulum

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.satelite.tasks import (
    slice_data,
    download,
    tratar_dados,
    salvar_parquet,
)
from pipelines.rj_cor.meteorologia.satelite.schedules import hour_schedule
from pipelines.utils.tasks import upload_to_gcs

with Flow("COR: Meteorologia - Satelite") as flow:
    # CURRENT_TIME = Parameter('CURRENT_TIME', default=pendulum.now("utc"))
    CURRENT_TIME = pendulum.now("UTC")

    ano, mes, dia, hora, dia_juliano = slice_data(current_time=CURRENT_TIME)

    # Para quantidade de água precipitável
    VARIAVEL = "TPWF"
    DATASET_ID = "meio_ambiente_clima"
    TABLE_ID = "satelite_quantidade_agua_precipitavel"

    filename = download(variavel=VARIAVEL, ano=ano, dia_juliano=dia_juliano, hora=hora)
    info = tratar_dados(filename=filename)
    path = salvar_parquet(info=info)
    upload_to_gcs(path=path, dataset_id=DATASET_ID, table_id=TABLE_ID)

    # Para taxa de precipitação
    VARIAVEL = "RRQPEF"
    DATASET_ID = "meio_ambiente_clima"
    TABLE_ID = "satelite_taxa_precipitacao"

    filename = download(variavel=VARIAVEL, ano=ano, dia_juliano=dia_juliano, hora=hora)
    info = tratar_dados(filename=filename)
    path = salvar_parquet(info=info)
    upload_to_gcs(path=path, dataset_id=DATASET_ID, table_id=TABLE_ID)

# para rodar na cloud
flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
flow.schedule = hour_schedule
