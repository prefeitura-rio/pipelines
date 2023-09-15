# -*- coding: utf-8 -*-
"""
Flows for rj_smtr
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect import case, Parameter
from prefect.tasks.control_flow import merge

# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
)

# SMTR Imports #

from pipelines.rj_smtr.tasks import (
    create_date_partition,
    create_date_hour_partition,
    create_local_partition_path,
    get_current_timestamp,
    get_raw,
    parse_timestamp_to_string,
    save_raw_local,
    save_treated_local,
    upload_logs_to_bq,
    bq_upload,
    transform_to_nested_structure,
)

from pipelines.rj_smtr.tasks import (
    create_request_params,
    get_datetime_range,
)

CAPTURA_GENERICO_FLOW_NAME = "SMTR: Captura Genérico"

with Flow(
    CAPTURA_GENERICO_FLOW_NAME,
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as captura_generico:
    # SETUP #

    table_params = Parameter("table_params", default=None)
    timestamp_param = Parameter("timestamp", default=None)
    interval = Parameter("interval", default=None)
    create_request_params_func = Parameter("create_request_params_func", default=None)
    dataset_id = Parameter("dataset_id", default=None)
    secret_path = Parameter("secret_path", default=None)

    timestamp = get_current_timestamp(timestamp_param)

    datetime_range = get_datetime_range(timestamp, interval=interval)

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=captura_generico.name + " " + table_params["table_id"] + ": ",
        now_time=timestamp,
    )

    request_params, url = create_request_params(
        datetime_range=datetime_range,
        table_params=table_params,
        secret_path=secret_path,
        create_request_params_func=create_request_params_func,
    )

    with case(table_params["flag_date_partition"], True):
        date_partitions = create_date_partition(timestamp)

    with case(table_params["flag_date_partition"], False):
        date_hour_partitions = create_date_hour_partition(timestamp)

    partitions = merge(date_partitions, date_hour_partitions)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=dataset_id,
        table_id=table_params["table_id"],
        filename=filename,
        partitions=partitions,
    )

    raw_status = get_raw(
        url=url,
        headers=secret_path,
        params=request_params,
    )

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

    # TREAT & CLEAN #
    treated_status = transform_to_nested_structure(
        status=raw_status,
        timestamp=timestamp,
        primary_key=table_params["primary_key"],
    )

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD #
    error = bq_upload(
        dataset_id=dataset_id,
        table_id=table_params["table_id"],
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )

    upload_logs_to_bq(
        dataset_id=dataset_id,
        parent_table_id=table_params["table_id"],
        error=error,
        timestamp=timestamp,
    )

captura_generico.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_generico.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
