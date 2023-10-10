# -*- coding: utf-8 -*-
"""
Flows for rj_smtr
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect import Parameter, case, unmapped
from prefect.tasks.control_flow import merge

# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import rename_current_flow_run_now_time, get_now_time

# SMTR Imports #

from pipelines.rj_smtr.tasks import (
    create_date_hour_partition,
    create_local_partition_path,
    get_current_timestamp,
    parse_timestamp_to_string,
    upload_raw_data_to_gcs,
    upload_staging_data_to_gcs,
    transform_raw_to_nested_structure,
    get_raw_from_sources,
    create_request_params,
    query_logs,
)


with Flow(
    "SMTR: Captura",
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as default_capture_flow:
    # Configuração #

    table_id = Parameter("table_id", default=None)
    partition_date_only = Parameter("partition_date_only", default=None)
    extract_params = Parameter("extract_params", default=None)
    dataset_id = Parameter("dataset_id", default=None)
    secret_path = Parameter("secret_path", default=None)
    primary_key = Parameter("primary_key", default=None)
    source_type = Parameter("source_type", default=None)
    recapture = Parameter("recapture", default=False)

    with case(recapture, True):
        _, recapture_timestamps, previous_errors = query_logs(
            dataset_id=dataset_id,
            table_id=table_id,
        )

    with case(recapture, False):
        capture_timestamp = [get_current_timestamp()]
        previous_errors = [None]

    timestamps = merge(recapture_timestamps, capture_timestamp)

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=default_capture_flow.name + " " + table_id + ": ",
        now_time=get_now_time(),
    )

    partitions = create_date_hour_partition.map(
        timestamps, partition_date_only=unmapped(partition_date_only)
    )

    filenames = parse_timestamp_to_string.map(timestamps)

    filepaths = create_local_partition_path.map(
        dataset_id=unmapped(dataset_id),
        table_id=unmapped(table_id),
        filename=filenames,
        partitions=partitions,
    )

    # Extração #
    request_params, request_paths = create_request_params.map(
        dataset_id=unmapped(dataset_id),
        extract_params=unmapped(extract_params),
        table_id=unmapped(table_id),
        timestamp=timestamps,
    )

    errors, raw_filepaths = get_raw_from_sources.map(
        source_type=unmapped(source_type),
        local_filepath=unmapped(filepaths),
        source_path=request_paths,
        dataset_id=unmapped(dataset_id),
        table_id=unmapped(table_id),
        secret_path=unmapped(secret_path),
        request_params=request_params,
    )

    errors = upload_raw_data_to_gcs.map(
        error=errors,
        raw_filepath=raw_filepaths,
        table_id=unmapped(table_id),
        dataset_id=unmapped(dataset_id),
        partitions=unmapped(partitions),
    )

    # Pré-tratamento #

    errors, staging_filepaths = transform_raw_to_nested_structure.map(
        raw_filepath=raw_filepaths,
        filepath=filepaths,
        error=errors,
        timestamp=timestamps,
        primary_key=unmapped(primary_key),
    )

    STAGING_UPLOADED = upload_staging_data_to_gcs.map(
        error=errors,
        staging_filepath=staging_filepaths,
        timestamp=timestamps,
        table_id=unmapped(table_id),
        dataset_id=unmapped(dataset_id),
        partitions=partitions,
        previous_error=previous_errors,
        recapture=recapture,
    )

default_capture_flow.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
default_capture_flow.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
