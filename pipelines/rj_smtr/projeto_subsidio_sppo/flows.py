# -*- coding: utf-8 -*-
"""
Flows for projeto_subsidio_sppo
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

# EMD Imports #

from pipelines.constants import constants
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
)
from pipelines.utils.decorators import Flow

# SMTR Imports #

from pipelines.rj_smtr.constants import constants as smtr_constants
from pipelines.rj_smtr.tasks import (
    create_date_hour_partition,
    # create_local_partition_path,
    # fetch_dataset_sha,
    get_current_timestamp,
    # get_local_dbt_client,
    parse_timestamp_to_string,
    # save_raw_local,
    # save_treated_local,
    # set_last_run_timestamp,
    upload_logs_to_bq,
    bq_upload,
)
from pipelines.rj_smtr.projeto_subsidio_sppo.tasks import (
    get_raw_and_save_local,
    pre_treatment_subsidio_sppo_gtfs,
)

# from pipelines.utils.execute_dbt_model.tasks import run_dbt_model
# from pipelines.rj_smtr.schedules import (
#     every_fortnight,
# )

with Flow(
    "SMTR - Subsidio: Planejado", code_owners=["fernanda", "rodrigo"]
) as subsidio_sppo_planejado:

    # SETUP
    # date_range_start = Parameter("date_range_start", default=None)
    # date_range_end = Parameter("date_range_end", default=None)
    # update_data = Parameter("update_data", default=False)

    # planejado_bucket = Parameter(
    #     "planejado_bucket", default=smtr_constants.SUBSIDIO_GTFS_ENDPOINTS.value
    # )

    timestamp = get_current_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix="SMTR - Subsidio Planejado:", now_time=timestamp
    )

    partitions = create_date_hour_partition(timestamp)

    filename = parse_timestamp_to_string(timestamp)

    # filepath = create_local_partition_path.map(
    #     dataset_id=unmapped(smtr_constants.SUBSIDIO_SPPO_PREPROD_DATASET_ID.value),
    #     table_id=smtr_constants.SUBSIDIO_SPPO_GTFS_TABLES.value,
    #     filename=unmapped(filename),
    #     partitions=unmapped(partitions),
    # )
    # Get data from GCS
    raw_filepath = get_raw_and_save_local(
        # url=smtr_constants.SUBSIDIO_SPPO_RAW_BUCKET_URL.value,
        table_id=smtr_constants.SUBSIDIO_SPPO_GTFS_TABLES.value,
        filename=filename,
        partitions=partitions,
    )
    # raw_filepath = save_raw_local.map(status=raw_status, file_path=filepath)

    # treated_status = pre_treatment_planejado.map(filepath=raw_filepath, timestamp=timestamp)

    # treated_filepath = save_treated_local.map(status=treated_status, file_path=filepath)

    # LOAD #
    # error = bq_upload.map(
    #     dataset_id=unmapped(smtr_constants.SUBSIDIO_SPPO_PREPROD_DATASET_ID.value),
    #     table_id=smtr_constants.SUBSIDIO_SPPO_GTFS_TABLES.value,
    #     filepath=treated_filepath,
    #     raw_filepath=raw_filepath,
    #     partitions=partitions,
    #     status=treated_status,
    # )

    # upload_logs_to_bq.map(
    #     dataset_id=unmapped(smtr_constants.SUBSIDIO_SPPO_PREPROD_DATASET_ID.value),
    #     parent_table_id=smtr_constants.SUBSIDIO_SPPO_GTFS_TABLES.value,
    #     error=error,
    #     timestamp=timestamp,
    # )


subsidio_sppo_planejado.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
subsidio_sppo_planejado.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow.schedule = fortnight


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
