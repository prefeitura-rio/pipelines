# -*- coding: utf-8 -*-
"""
Flows for projeto_subsidio_sppo
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import merge

# from prefect.utilities.edges import unmapped

# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.tasks import (
    # rename_current_flow_run_now_time,
    get_now_day,
    # get_current_flow_mode,
    # get_current_flow_labels,
    # log
)

from pipelines.utils.decorators import Flow

# from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client

# SMTR Imports #

from pipelines.rj_smtr.constants import constants as smtr_constants

# from pipelines.rj_smtr.tasks import (
# create_date_hour_partition,
# create_local_partition_path,
# fetch_dataset_sha,
# get_current_timestamp,
# get_local_dbt_client,
# parse_timestamp_to_string,
# save_raw_local,
# save_treated_local,
# set_last_run_timestamp,
# upload_logs_to_bq,
# bq_upload,
# get_materialization_date_range,
# )
from pipelines.rj_smtr.projeto_subsidio_sppo.tasks import get_gtfs_version

# from pipelines.utils.execute_dbt_model.tasks import run_dbt_model
from pipelines.rj_smtr.schedules import (
    every_day_hour_five,
)

# with Flow(
#     "SMTR - Subsidio: Apuração", code_owners=["fernanda", "rodrigo"]
# ) as subsidio_sppo_apuracao:

#     date_range_start = Parameter("date_range_start", default=None)
#     date_range_end = Parameter("date_range_end", default=None)

#     timestamp = get_current_timestamp()

# 1. Verifica se existe o planejado para o período em preprod.

# 1.1 Se não existe, roda o flow planejado. Se não tiver planejado
# do período, não roda o flow de apuração.

# 1.2 Se existe, copia para produção.

# 2. Verifica se todas datas da quinzena estão com a versão correta
#    retorna as que devem ser corrigidas
# date_range_to_update = get_date_range(timestamp, date_range_start, date_range_end)

# 2. Atualiza as datas que precisam ser corrigidas

# Apuração quinzenal
# 1. Puxa ultima versao do planejado
# 1. Puxa dados de viagens apuradas

with Flow(
    "SMTR - Subsidio: Viagens", code_owners=["fernanda", "rodrigo"]
) as subsidio_sppo_viagens:

    # Get default parameters #
    dataset_id = Parameter(
        "dataset_id", default=smtr_constants.SUBSIDIO_SPPO_PREPROD_DATASET_ID.value
    )
    table_id = Parameter(
        "table_id", default=smtr_constants.SUBSIDIO_SPPO_PREPROD_TABLE_ID.value
    )
    rebuild = Parameter("rebuild", default=False)
    run_date = Parameter("run_date", default=False)
    gtfs_version = Parameter("gtfs_version", default=False)

    with case(run_date, False):
        default_date = get_now_day()
    with case(run_date, not False):
        param_date = run_date

    run_date = merge(default_date, param_date)

    with case(gtfs_version, False):
        default_version = get_gtfs_version(run_date)
    with case(run_date, True):
        param_version = gtfs_version

    gtfs_version = merge(default_version, default_version)

    # Rename flow run
    # rename_flow_run = rename_current_flow_run_now_time(
    #     prefix="SMTR - Subsidio Viagens: ", now_time=run_date
    # )

    # LABELS = get_current_flow_labels()
    # MODE = get_current_flow_mode(LABELS)

    # Set dbt client #
    # dbt_client = get_k8s_dbt_client(mode=MODE, wait=rename_flow_run)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    # Set specific run parameters #
    # date_range = get_materialization_date_range(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     # raw_dataset_id=raw_dataset_id,
    #     # raw_table_id=raw_table_id,
    #     table_date_column_name="data",
    #     mode=MODE,
    #     delay_hours=smtr_constants.SUBSIDIO_SPPO_PREPROD_DELAY_HOURS.value,
    # )

    # dataset_sha = fetch_dataset_sha(
    #     dataset_id=dataset_id,
    # )

    # # Run materialization #
    # RUN = run_dbt_model(
    #     dbt_client=dbt_client,
    #     model="viagem_planejada", #TODO: check
    #     upstream=True,
    #     #exclude="br_rj_riodejaneiro_sigmob",
    #     _vars=[
    #         {"run_date": run_date},
    #         {"timestamp_captura_gtfs": timestamp_captura_gtfs},
    #         dataset_sha,
    #     ],
    # )

    # set_last_run_timestamp(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     timestamp=date_range["date_range_end"],
    #     wait=RUN,
    #     mode=MODE,
    # )

subsidio_sppo_viagens.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
subsidio_sppo_viagens.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
subsidio_sppo_viagens.schedule = every_day_hour_five
