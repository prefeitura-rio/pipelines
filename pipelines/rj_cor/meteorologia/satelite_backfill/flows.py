# -*- coding: utf-8 -*-
"""
Flows for emd
"""

from prefect import Flow, Parameter, task
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.satelite.tasks import (
    slice_data,
    download,
    tratar_dados,
    salvar_parquet,
)
from pipelines.rj_cor.meteorologia.satelite_backfill.tasks import delete_files
from pipelines.utils.tasks import create_table_and_upload_to_gcs

# from prefect.core import Edge


@task
def print_param(param):
    """
    print
    """
    print("\n\n\n>>>>>>>>>>>>>>>>>>>>> HORA DA VEZ: ", param, "\n\n\n")
    return param


with Flow("satelite_goes_16_backfill") as satelite_goes_16_backfill:

    CURRENT_TIME = Parameter("CURRENT_TIME", default=None)
    VARIAVEL_TPW = "TPWF"
    DATASET_ID_TPW = "meio_ambiente_clima"
    TABLE_ID_TPW = "quantidade_agua_precipitavel_satelite"
    DUMP_MODE = "append"

    print_param(CURRENT_TIME)

    ano, mes, dia, hora, dia_juliano = slice_data(CURRENT_TIME)

    filename_tpw = download(
        variavel=VARIAVEL_TPW, ano=ano, dia_juliano=dia_juliano, hora=hora
    )

    info_tpw = tratar_dados(filename=filename_tpw)
    path_tpw, file_tpw = salvar_parquet(info=info_tpw)

    waiting_tpw = create_table_and_upload_to_gcs(
        data_path=path_tpw,
        dataset_id=DATASET_ID_TPW,
        table_id=TABLE_ID_TPW,
        dump_mode=DUMP_MODE,
        wait=path_tpw,
    )
    # Edge(upstream_task = create_table_and_upload_to_gcs,
    #                                downstream_task = delete_files)
    delete_files(filename=file_tpw, input_filename=filename_tpw, wait=waiting_tpw)

    VARIAVEL_RR = "RRQPEF"
    DATASET_ID_RR = "meio_ambiente_clima"
    TABLE_ID_RR = "taxa_precipitacao_satelite"

    filename_rr = download(
        variavel=VARIAVEL_RR, ano=ano, dia_juliano=dia_juliano, hora=hora
    )

    info_rr = tratar_dados(filename=filename_rr)
    path_rr, file_rr = salvar_parquet(info=info_rr)

    waiting_rr = create_table_and_upload_to_gcs(
        data_path=path_rr,
        dataset_id=DATASET_ID_RR,
        table_id=TABLE_ID_RR,
        dump_mode=DUMP_MODE,
        wait=path_rr,
    )
    # Edge(upstream_task = create_table_and_upload_to_gcs,
    #                                downstream_task = delete_files)
    delete_files(filename=file_rr, input_filename=filename_rr, wait=waiting_rr)

# para rodar na cloud
satelite_goes_16_backfill.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
satelite_goes_16_backfill.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# satelite_goes_16_backfill.add_edge(upstream_task = create_table_and_upload_to_gcs,
#                                    downstream_task = delete_files)
