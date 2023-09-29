# -*- coding: utf-8 -*-
"""
Flows for gtfs
"""
from copy import deepcopy

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

# EMD Imports #

from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from pipelines.constants import constants as constants_emd
from pipelines.utils.decorators import Flow

# SMTR Imports #

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.tasks import (
    create_date_hour_partition,
    create_local_partition_path,
    parse_timestamp_to_string,
    upload_logs_to_bq,
    bq_upload,
    save_raw_local,
)
from pipelines.rj_smtr.br_rj_riodejaneiro_gtfs.tasks import (
    download_gtfs,
    get_current_timestamp_from_date,
)
from pipelines.rj_smtr.schedules import *

from pipelines.rj_smtr.flows import default_capture_flow

# FLOW 1: Captura zip, download, unzip e disponibiliza numa URL (GCS) -> Simula uma API

# Testar para ver se está funcionando OK!
'''
with Flow(
    "SMTR: GTFS (pré-captura)",
    code_owners=["rodrigo", "carol"],
) as download_gtfs_flow:
    # SETUP
    date = Parameter("date", default=None)  # "data da captura"
    feed_start_date = Parameter("feed_start_date", default=None)
    """
    # passar para o proximo flow. TODO: ver onde tem que ser passada
    # essa informação (pre-tratar nesse flow? passar para o DBT?)
    """
    feed_end_date = Parameter("feed_end_date", default=None)
    """
    # passar para o proximo flow. TODO: ver onde tem que ser passada
    # essa informação (pre-tratar nesse flow? passar para o DBT?)
    """
    timestamp = get_current_timestamp_from_date(date)
    """
    # rename_flow_run = rename_current_flow_run_now_time(
    #     prefix="SMTR - GTFS Captura:", now_time=timestamp
    # )
    """
    partitions = create_date_hour_partition(timestamp)

    filename = parse_timestamp_to_string(timestamp)

    # Get data from GCS
    mapped_tables_status = download_gtfs()  # downlaod_and_pre_treat

    filepath = create_local_partition_path.map(
        dataset_id=unmapped(constants.GTFS_DATASET_ID.value),
        table_id=mapped_tables_status["table_id"],
        filename=unmapped(filename),
        partitions=unmapped(partitions),
    )

    treated_raw_filepath = save_raw_local.map(
        file_path=filepath,
        status=mapped_tables_status["status"],
        mode=unmapped("raw"),
    )

    """
    pre_treatment = transform_to_nested_structure.map(
        status=mapped_tables_status,
        timestamp=timestamp,
        primary_key=constants.GTFS_TABLE_PARAMS.value,
    )
    """

    # LOAD #
    errors = bq_upload.map(
        filepath=treated_raw_filepath,
        dataset_id=unmapped(constants.GTFS_DATASET_ID.value),
        table_id=mapped_tables_status["table_id"],
        partitions=unmapped(partitions),
        status=mapped_tables_status["status"],
    )

    UPLOAD_LOGS = upload_logs_to_bq.map(
        dataset_id=unmapped(constants.GTFS_DATASET_ID.value),
        parent_table_id=constants.GTFS_TABLES.value,
        error=errors,
        timestamp=unmapped(timestamp),
    )

download_gtfs_flow.storage = GCS(constants_emd.GCS_FLOWS_BUCKET.value)
download_gtfs_flow.run_config = KubernetesRun(
    image=constants_emd.DOCKER_IMAGE.value,
    labels=[constants_emd.RJ_SMTR_DEV_AGENT_LABEL.value],
)
'''
"""
# FLOW 2: Captura e aninhamento do dado

# Bucket:
# - raw: txt na particao correta
# - staging: csv aninhado na particao correta

# BILHETAGEM PRINCIPAL - CAPTURA DIÁRIA DE DIVERSAS TABELAS #

# TODO: Refatorar para rodar sem schedule ligado e chamar no flow anterior

# gtfs_captura = deepcopy(default_capture_flow)
# gtfs_captura.name = "SMTR: GTFS (captura)"
# gtfs_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
# gtfs_captura.run_config = KubernetesRun(
#     image=emd_constants.DOCKER_IMAGE.value,
#     labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
# )
# gtfs_captura.schedule = gtfs_captura_schedule
"""

# testar e checar que se a construção do flow funciona corretamente #

with Flow(
    "SMTR - Captura e tratamento de dados do GTFS",
    code_owners=["rodrigo", "carol"],
) as gtfs_captura:
    # SETUP

    gtfs_captura = deepcopy(default_capture_flow)
    gtfs_captura.name = "SMTR - Captura e tratamento de dados do GTFS"
    gtfs_captura.storage = GCS(constants_emd.GCS_FLOWS_BUCKET.value)
    gtfs_captura.run_config = KubernetesRun(
        image=constants_emd.DOCKER_IMAGE.value,
        labels=[constants_emd.RJ_SMTR_DEV_AGENT_LABEL.value],
    )
    """
    with case(download_gtfs, True):
        parameters = {
            "date": Parameter("date", default=None),
            "feed_start_date": Parameter("feed_start_date", default=None),
            "feed_end_date": Parameter("feed_end_date", default=None),
        }
        GTFS_CAPTURA_RUN = create_flow_run(
            flow_name=download_gtfs.name,
            project_name=constants.GTFS_DATASET_ID.value,
            run_name=download_gtfs.name,
            parameters=parameters,
        )
        GTFS_CAPTURA_WAIT = wait_for_flow_run(
            GTFS_CAPTURA_RUN,
            stream_logs=True,
            stream_states=True,
        )
    """
# download_gtfs.schedule = None

# gtfs_captura.schedule = None
