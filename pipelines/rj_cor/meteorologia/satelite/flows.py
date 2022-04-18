# -*- coding: utf-8 -*-
"""
Flows for emd
"""
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.satelite.tasks import (
    get_dates,
    slice_data,
    download,
    tratar_dados,
    salvar_parquet,
)
from pipelines.rj_cor.meteorologia.satelite.schedules import hour_schedule
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
)

with Flow(
    name="COR: Meteorologia - Satelite GOES 16",
    code_owners=[
        "@PatyBC#4954",
    ],
) as cor_meteorologia_goes16:

    CURRENT_TIME = get_dates()

    ano, mes, dia, hora, dia_juliano = slice_data(current_time=CURRENT_TIME)

    # Para taxa de precipitação
    VARIAVEL_RR = "RRQPEF"
    DATASET_ID_RR = "meio_ambiente_clima"
    TABLE_ID_RR = "taxa_precipitacao_satelite"
    DUMP_TYPE = "append"

    filename_rr = download(
        variavel=VARIAVEL_RR, ano=ano, dia_juliano=dia_juliano, hora=hora
    )

    info_rr = tratar_dados(filename=filename_rr)
    path_rr = salvar_parquet(info=info_rr)

    # Create table in BigQuery
    create_table_and_upload_to_gcs(
        data_path=path_rr,
        dataset_id=DATASET_ID_RR,
        table_id=TABLE_ID_RR,
        dump_type=DUMP_TYPE,
        wait=path_rr,
    )

    # Para quantidade de água precipitável
    VARIAVEL_TPW = "TPWF"
    DATASET_ID_TPW = "meio_ambiente_clima"
    TABLE_ID_TPW = "volume_agua_precipitavel_satelite"

    filename_tpw = download(
        variavel=VARIAVEL_TPW, ano=ano, dia_juliano=dia_juliano, hora=hora
    )

    info_tpw = tratar_dados(filename=filename_tpw)
    path_tpw = salvar_parquet(info=info_tpw)

    create_table_and_upload_to_gcs(
        data_path=path_tpw,
        dataset_id=DATASET_ID_TPW,
        table_id=TABLE_ID_TPW,
        dump_type=DUMP_TYPE,
        wait=path_tpw,
    )

# para rodar na cloud
cor_meteorologia_goes16.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16.schedule = hour_schedule
