# -*- coding: utf-8 -*-
"""
Flows for gtfs
"""
from copy import deepcopy
from datetime import datetime
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

# EMD Imports #

from pipelines.utils.decorators import Flow

# SMTR Imports #

from pipelines.rj_smtr.constants import constants as constants
from pipelines.rj_smtr.tasks import (
    create_date_hour_partition,
    create_local_partition_path,
    parse_timestamp_to_string,
    upload_logs_to_bq,
    bq_upload
)
from pipelines.rj_smtr.br_rj_riodejaneiro_gtfs.tasks import (
    get_current_timestamp_from_date,
    download_gtfs
)

from pipelines.rj_smtr.flows import default_capture_flow

# FLOW 1: Captura zip, download, unzip e disponibiliza numa URL (GCS) -> Simula uma API

## TODO: Testar para ver se está funcionando OK!

with Flow(
    "SMTR: GTFS (pré-captura)",
    code_owners=["rodrigo", "carol"],
) as download_gtfs_flow:   # TBD nome mais explicito
    
    # SETUP
    date = Parameter("date", default=None) # "data da captura"
    feed_start_date = Parameter("feed_start_date", default=None)
    # passar para o proximo flow. TODO: ver onde tem que ser passada
    # essa informação (pre-tratar nesse flow? passar para o DBT?)
    feed_end_date = Parameter("feed_end_date", default=None) 
    # passar para o proximo flow. TODO: ver onde tem que ser passada
    # essa informação (pre-tratar nesse flow? passar para o DBT?)

    timestamp = get_current_timestamp_from_date(date)

    # rename_flow_run = rename_current_flow_run_now_time(
    #     prefix="SMTR - GTFS Captura:", now_time=timestamp
    # )

    partitions = create_date_hour_partition(timestamp)

    filename = parse_timestamp_to_string(timestamp)

    # Get data from GCS
    mapped_tables_status = download_gtfs() # downlaod_and_pre_treat

    filepath = create_local_partition_path.map(
        dataset_id=unmapped(constants.GTFS_DATASET_ID.value),
        table_id=mapped_tables_status["table_id"],
        filename=unmapped(filename),
        partitions=unmapped(partitions),
    )

    # LOAD #
    error = bq_upload.map(
        dataset_id=unmapped(constants.GTFS_DATASET_ID.value),
        table_id=mapped_tables_status["table_id"],
        filepath=filepath,
        partitions=unmapped(partitions),
        status=mapped_tables_status["status"]
    )

    UPLOAD_LOGS = upload_logs_to_bq.map(
        dataset_id=unmapped(constants.GTFS_DATASET_ID.value),
        parent_table_id=constants.GTFS_TABLES.value,
        error=error,
        timestamp=unmapped(timestamp),
    )

download_gtfs_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
download_gtfs_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)

# FLOW 2: Captura e aninhamento do dado

# Bucket:
# - raw: txt na particao correta
# - staging: csv aninhado na particao correta

# BILHETAGEM PRINCIPAL - CAPTURA DIÁRIA DE DIVERSAS TABELAS #

## TODO: Refatorar para rodar sem schedule ligado e chamar no flow anterior

# gtfs_captura = deepcopy(default_capture_flow)
# gtfs_captura.name = "SMTR: GTFS (captura)"
# gtfs_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
# gtfs_captura.run_config = KubernetesRun(
#     image=emd_constants.DOCKER_IMAGE.value,
#     labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
# )
# gtfs_captura.schedule = gtfs_captura_schedule