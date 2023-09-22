# -*- coding: utf-8 -*-
"""
Flows for rj_smtr
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect import case, Parameter
from prefect.tasks.control_flow import merge
from prefect.utilities.edges import unmapped

# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
    get_now_time,
    get_current_flow_labels,
    get_current_flow_mode,
)
from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client

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
    create_dbt_run_vars,
    set_last_run_timestamp,
    treat_dbt_table_params,
    coalesce_task,
)

from pipelines.rj_smtr.tasks import (
    create_request_params,
    get_datetime_range,
)

from pipelines.utils.execute_dbt_model.tasks import run_dbt_model

with Flow(
    "SMTR: Captura",
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as default_capture_flow:
    # SETUP #

    table_params = Parameter("table_params", default=None)
    timestamp_param = Parameter("timestamp", default=None)
    interval = Parameter("interval", default=None)
    dataset_id = Parameter("dataset_id", default=None)
    secret_path = Parameter("secret_path", default=None)

    timestamp = get_current_timestamp(timestamp_param)

    datetime_range = get_datetime_range(timestamp, interval=interval)

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=default_capture_flow.name + " " + table_params["table_id"] + ": ",
        now_time=timestamp,
    )

    request_params, request_url = create_request_params(
        datetime_range=datetime_range,
        table_params=table_params,
        secret_path=secret_path,
        dataset_id=dataset_id,
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
        url=request_url,
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

default_capture_flow.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
default_capture_flow.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

with Flow(
    "SMTR: Materialização",
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as default_materialization_flow:
    # SETUP #

    dataset_id = Parameter("dataset_id", default=None)
    table_params = Parameter("table_params", default=dict())

    treated_table_params = treat_dbt_table_params(table_params=table_params)

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    dbt_client = get_k8s_dbt_client(mode=MODE, wait=rename_flow_run)

    _vars, date_var, flag_date_range = create_dbt_run_vars(
        dataset_id=dataset_id,
        var_params=treated_table_params["var_params"],
        table_id=treated_table_params["table_id"],
        raw_dataset_id=dataset_id,
        raw_table_id=treated_table_params["raw_table_id"],
        mode=MODE,
    )

    # Rename flow run

    flow_name_prefix = coalesce_task([treated_table_params["table_id"], dataset_id])

    flow_name_now_time = coalesce_task([date_var, get_now_time()])

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=default_materialization_flow.name + " " + flow_name_prefix + ": ",
        now_time=flow_name_now_time,
        wait=flow_name_prefix
    )

    RUNS = run_dbt_model.map(
        dbt_client=unmapped(dbt_client),
        dataset_id=unmapped(dataset_id),
        table_id=unmapped(treated_table_params["table_id"]),
        _vars=_vars,
        dbt_alias=unmapped(treated_table_params["dbt_alias"]),
        upstream=unmapped(treated_table_params["upstream"]),
        downstream=unmapped(treated_table_params["downstream"]),
        exclude=unmapped(treated_table_params["exclude"]),
        flags=unmapped(treated_table_params["flags"]),
    )

    with case(flag_date_range, True):
        set_last_run_timestamp(
            dataset_id=dataset_id,
            table_id=treated_table_params["table_id"],
            timestamp=date_var["date_range_end"],
            wait=RUNS,
            mode=MODE,
        )


default_materialization_flow.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
default_materialization_flow.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
