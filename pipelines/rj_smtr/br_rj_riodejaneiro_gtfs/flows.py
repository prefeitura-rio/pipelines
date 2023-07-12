# -*- coding: utf-8 -*-
"""
Flows for gtfs
"""
from datetime import datetime
from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

# EMD Imports #

from pipelines.constants import constants
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
    get_current_flow_mode,
    get_current_flow_labels,
)

from pipelines.utils.decorators import Flow

# SMTR Imports #

from pipelines.rj_smtr.constants import constants as smtr_constants
from pipelines.rj_smtr.tasks import (
    create_date_partition,
    create_local_partition_path,
    # fetch_dataset_sha,
    get_current_timestamp,
    # get_local_dbt_client,
    parse_timestamp_to_string,
    # save_raw_local,
    save_treated_local,
    set_last_run_timestamp,
    upload_logs_to_bq,
    bq_upload,
)
from pipelines.rj_smtr.br_rj_riodejaneiro_gtfs.tasks import (
    get_raw_gtfs,
    save_raw_local_gtfs,
    pre_treatment_gtfs,
    get_current_timestamp_from_date,
)

# from pipelines.utils.execute_dbt_model.tasks import run_dbt_model
# from pipelines.rj_smtr.schedules import (
#     every_fortnight,
# )

with Flow(
    "[TESTE] SMTR - GTFS: Captura",
    code_owners=["rodrigo"],
) as gtfs_captura:  # "caio", "fernanda", "boris",

    # SETUP
    date = Parameter("date", default=None)

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    timestamp = get_current_timestamp_from_date(date)

    # rename_flow_run = rename_current_flow_run_now_time(
    #     prefix="SMTR - GTFS Captura:", now_time=timestamp
    # )

    partitions = create_date_partition(timestamp, date_var="data_versao")

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path.map(
        dataset_id=unmapped(smtr_constants.GTFS_DATASET_ID.value),
        table_id=smtr_constants.GTFS_TABLES.value,
        filename=unmapped(filename),
        partitions=unmapped(partitions),
    )

    # Get data from GCS
    raw_status = get_raw_gtfs()

    raw_filepath = save_raw_local_gtfs.map(
        filepath=filepath, status=unmapped(raw_status)
    )

    treated_status = pre_treatment_gtfs.map(
        status=unmapped(raw_status),
        filepath=raw_filepath,
        timestamp=unmapped(timestamp),
    )

    treated_filepath = save_treated_local.map(status=treated_status, file_path=filepath)

    # LOAD #
    error = bq_upload.map(
        dataset_id=unmapped(smtr_constants.GTFS_DATASET_ID.value),
        table_id=smtr_constants.GTFS_TABLES.value,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=unmapped(partitions),
        status=treated_status,
    )

    UPLOAD_LOGS = upload_logs_to_bq.map(
        dataset_id=unmapped(smtr_constants.GTFS_DATASET_ID.value),
        parent_table_id=smtr_constants.GTFS_TABLES.value,
        error=error,
        timestamp=unmapped(timestamp),
    )

    set_last_run_timestamp.map(
        dataset_id=unmapped(smtr_constants.GTFS_DATASET_ID.value),
        table_id=smtr_constants.GTFS_TABLES.value,
        timestamp=unmapped(raw_status["gtfs_last_modified"]),
        wait=unmapped(UPLOAD_LOGS),
        mode=unmapped(MODE),
    )

gtfs_captura.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
gtfs_captura.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
# flow.schedule = fortnight
