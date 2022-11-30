# -*- coding: utf-8 -*-
"""
Flows for gtfs
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
    create_local_partition_path,
    # fetch_dataset_sha,
    get_current_timestamp,
    # get_local_dbt_client,
    parse_timestamp_to_string,
    # save_raw_local,
    save_treated_local,
    # set_last_run_timestamp,
    upload_logs_to_bq,
    bq_upload,
)
from pipelines.rj_smtr.gtfs.tasks import (
    get_raw,
    save_raw_local,
    pre_treatment_subsidio_gtfs,
)

# from pipelines.utils.execute_dbt_model.tasks import run_dbt_model
# from pipelines.rj_smtr.schedules import (
#     every_fortnight,
# )

with Flow("SMTR - GTFS: Captura", code_owners=["fernanda"]) as gtfs_captura:

    # SETUP
    timestamp = Parameter("timestamp", default=None)

    timestamp = get_current_timestamp(timestamp)

    rename_flow_run = rename_current_flow_run_now_time(
        prefix="SMTR - GTFS Captura:", now_time=timestamp
    )

    partitions = create_date_hour_partition(timestamp)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path.map(
        dataset_id=unmapped(smtr_constants.GTFS_DATASET_ID.value),
        table_id=smtr_constants.GTFS_TABLES.value,
        filename=unmapped(filename),
        partitions=unmapped(partitions),
    )

    # Get data from GCS
    raw_status = get_raw()

    raw_filepath = save_raw_local.map(filepath=filepath, status=unmapped(raw_status))

    treated_status = pre_treatment_subsidio_gtfs.map(
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

    upload_logs_to_bq.map(
        dataset_id=unmapped(smtr_constants.GTFS_DATASET_ID.value),
        parent_table_id=smtr_constants.GTFS_TABLES.value,
        error=error,
        timestamp=unmapped(timestamp),
    )

gtfs_captura.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
gtfs_captura.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow.schedule = fortnight
