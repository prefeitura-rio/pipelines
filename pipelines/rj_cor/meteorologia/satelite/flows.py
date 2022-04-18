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
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
)

with Flow("COR: Meteorologia - Satelite GOES 16") as cor_meteorologia_goes16:
    CURRENT_TIME = pendulum.now("UTC")

    ano, mes, dia, hora, dia_juliano = slice_data(current_time=CURRENT_TIME)

    # Para taxa de precipitação
    VARIAVEL = "RRQPEF"
    DATASET_ID = "meio_ambiente_clima"
    TABLE_ID = "satelite_taxa_precipitacao"
    DUMP_TYPE = "append"

    filename = download(variavel=VARIAVEL, ano=ano, dia_juliano=dia_juliano, hora=hora)
    info = tratar_dados(filename=filename)
    path, partitions = salvar_parquet(info=info)

    # Create table in BigQuery
    create_table_and_upload_to_gcs(
        data_path=path,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dump_type=DUMP_TYPE,
        partitions=partitions,
        wait=path,
    )

    # Para quantidade de água precipitável
    VARIAVEL = "TPWF"
    DATASET_ID = "meio_ambiente_clima"
    TABLE_ID = "satelite_quantidade_agua_precipitavel"

    filename = download(variavel=VARIAVEL, ano=ano, dia_juliano=dia_juliano, hora=hora)
    info = tratar_dados(filename=filename)
    path, partitions = salvar_parquet(info=info)

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
cor_meteorologia_goes16.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
cor_meteorologia_goes16.schedule = hour_schedule
