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
    VARIAVEL_RR = "RRQPEF"
    DATASET_ID_RR = "meio_ambiente_clima"
    TABLE_ID_RR = "satelite_taxa_precipitacao"
    DUMP_TYPE = "append"

    filename_rr = download(
        variavel=VARIAVEL_RR, ano=ano, dia_juliano=dia_juliano, hora=hora
    )

    info_rr = tratar_dados(filename=filename_rr)
    path_rr, partitions_rr = salvar_parquet(info=info_rr)

    # Create table in BigQuery
    create_table_and_upload_to_gcs(
        data_path=path_rr,
        dataset_id=DATASET_ID_RR,
        table_id=TABLE_ID_RR,
        dump_type=DUMP_TYPE,
        partitions=partitions_rr,
        wait=path_rr,
    )

    # Para quantidade de água precipitável
    VARIAVEL_TPW = "TPWF"
    DATASET_ID_TPW = "meio_ambiente_clima"
    TABLE_ID_TPW = "satelite_quantidade_agua_precipitavel"

    filename_tpw = download(
        variavel=VARIAVEL_TPW, ano=ano, dia_juliano=dia_juliano, hora=hora
    )

    info_tpw = tratar_dados(filename=filename_tpw)
    path_tpw, partitions_tpw = salvar_parquet(info=info_tpw)

    create_table_and_upload_to_gcs(
        data_path=path_tpw,
        dataset_id=DATASET_ID_TPW,
        table_id=TABLE_ID_TPW,
        dump_type=DUMP_TYPE,
        partitions=partitions_tpw,
        wait=path_tpw,
    )

# para rodar na cloud
cor_meteorologia_goes16.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16.schedule = hour_schedule
