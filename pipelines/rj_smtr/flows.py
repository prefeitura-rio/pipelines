# -*- coding: utf-8 -*-
"""
Flows for rj_smtr
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect import case, Parameter, task
from prefect.utilities.edges import unmapped
from prefect.tasks.control_flow import merge

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
    create_date_hour_partition,
    create_local_partition_path,
    get_rounded_timestamp,
    parse_timestamp_to_string,
    transform_raw_to_nested_structure,
    create_dbt_run_vars,
    set_last_run_timestamp,
    coalesce_task,
    upload_raw_data_to_gcs,
    upload_staging_data_to_gcs,
    get_raw_from_sources,
    create_request_params,
    query_logs,
    unpack_mapped_results_nout2,
)

from pipelines.utils.execute_dbt_model.tasks import run_dbt_model

with Flow(
    "SMTR: Captura",
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as default_capture_flow:
    # Configuração #

    # Parâmetros Gerais #
    table_id = Parameter("table_id", default=None)
    dataset_id = Parameter("dataset_id", default=None)
    partition_date_only = Parameter("partition_date_only", default=None)

    # Parâmetros Captura #
    extract_params = Parameter("extract_params", default=None)
    secret_path = Parameter("secret_path", default=None)
    source_type = Parameter("source_type", default=None)
    interval_minutes = Parameter("interval_minutes", default=None)
    recapture = Parameter("recapture", default=False)
    recapture_window_days = Parameter("recapture_window_days", default=1)

    # Parâmetros Pré-tratamento #
    primary_key = Parameter("primary_key", default=None)

    get_run_name_prefix = task(
        lambda recap: "Recaptura" if recap else "Captura",
        name="get_run_name_prefix",
        checkpoint=False,
    )

    current_timestamp = get_rounded_timestamp(interval_minutes=interval_minutes)

    with case(recapture, True):
        _, recapture_timestamps, recapture_previous_errors = query_logs(
            dataset_id=dataset_id,
            table_id=table_id,
            datetime_filter=current_timestamp,
            interval_minutes=interval_minutes,
            recapture_window_days=recapture_window_days,
        )

    with case(recapture, False):
        capture_timestamp = [current_timestamp]
        capture_previous_errors = task(
            lambda: [None], checkpoint=False, name="assign_none_to_previous_errors"
        )()

    timestamps = merge(recapture_timestamps, capture_timestamp)
    previous_errors = merge(recapture_previous_errors, capture_previous_errors)

    rename_flow_run = rename_current_flow_run_now_time(
        prefix="SMTR: " + get_run_name_prefix(recap=recapture) + " " + table_id + ": ",
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
    create_request_params_returns = create_request_params.map(
        dataset_id=unmapped(dataset_id),
        extract_params=unmapped(extract_params),
        table_id=unmapped(table_id),
        timestamp=timestamps,
        interval_minutes=unmapped(interval_minutes),
    )

    request_params, request_paths = unpack_mapped_results_nout2(
        mapped_results=create_request_params_returns
    )

    get_raw_from_sources_returns = get_raw_from_sources.map(
        source_type=unmapped(source_type),
        local_filepath=filepaths,
        source_path=request_paths,
        dataset_id=unmapped(dataset_id),
        table_id=unmapped(table_id),
        secret_path=unmapped(secret_path),
        request_params=request_params,
    )

    errors, raw_filepaths = unpack_mapped_results_nout2(
        mapped_results=get_raw_from_sources_returns
    )

    errors = upload_raw_data_to_gcs.map(
        error=errors,
        raw_filepath=raw_filepaths,
        table_id=unmapped(table_id),
        dataset_id=unmapped(dataset_id),
        partitions=partitions,
    )

    # Pré-tratamento #

    nested_structure_returns = transform_raw_to_nested_structure.map(
        raw_filepath=raw_filepaths,
        filepath=filepaths,
        error=errors,
        timestamp=timestamps,
        primary_key=unmapped(primary_key),
    )

    errors, staging_filepaths = unpack_mapped_results_nout2(
        mapped_results=nested_structure_returns
    )

    STAGING_UPLOADED = upload_staging_data_to_gcs.map(
        error=errors,
        staging_filepath=staging_filepaths,
        timestamp=timestamps,
        table_id=unmapped(table_id),
        dataset_id=unmapped(dataset_id),
        partitions=partitions,
        previous_error=previous_errors,
        recapture=unmapped(recapture),
    )

default_capture_flow.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
default_capture_flow.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)

with Flow(
    "SMTR: Materialização",
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as default_materialization_flow:
    # SETUP #

    dataset_id = Parameter("dataset_id", default=None)
    table_id = Parameter("table_id", default=None)
    raw_table_id = Parameter("raw_table_id", default=None)
    dbt_alias = Parameter("dbt_alias", default=False)
    upstream = Parameter("upstream", default=None)
    downstream = Parameter("downstream", default=None)
    exclude = Parameter("exclude", default=None)
    flags = Parameter("flags", default=None)
    dbt_vars = Parameter("dbt_vars", default=dict())

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    _vars, date_var, flag_date_range = create_dbt_run_vars(
        dataset_id=dataset_id,
        dbt_vars=dbt_vars,
        table_id=table_id,
        raw_dataset_id=dataset_id,
        raw_table_id=raw_table_id,
        mode=MODE,
    )

    # Rename flow run

    flow_name_prefix = coalesce_task([table_id, dataset_id])

    flow_name_now_time = coalesce_task([date_var, get_now_time()])

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=default_materialization_flow.name + " " + flow_name_prefix + ": ",
        now_time=flow_name_now_time,
    )

    dbt_client = get_k8s_dbt_client(mode=MODE, wait=rename_flow_run)

    RUNS = run_dbt_model.map(
        dbt_client=unmapped(dbt_client),
        dataset_id=unmapped(dataset_id),
        table_id=unmapped(table_id),
        _vars=_vars,
        dbt_alias=unmapped(dbt_alias),
        upstream=unmapped(upstream),
        downstream=unmapped(downstream),
        exclude=unmapped(exclude),
        flags=unmapped(flags),
    )

    with case(flag_date_range, True):
        set_last_run_timestamp(
            dataset_id=dataset_id,
            table_id=table_id,
            timestamp=date_var["date_range_end"],
            wait=RUNS,
            mode=MODE,
        )


default_materialization_flow.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
default_materialization_flow.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

with Flow(
    "SMTR: Materialização",
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as default_materialization_flow:
    # SETUP #

    dataset_id = Parameter("dataset_id", default=None)
    table_id = Parameter("table_id", default=None)
    raw_table_id = Parameter("raw_table_id", default=None)
    dbt_alias = Parameter("dbt_alias", default=False)
    upstream = Parameter("upstream", default=None)
    downstream = Parameter("downstream", default=None)
    exclude = Parameter("exclude", default=None)
    flags = Parameter("flags", default=None)
    dbt_vars = Parameter("dbt_vars", default=dict())

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    _vars, date_var, flag_date_range = create_dbt_run_vars(
        dataset_id=dataset_id,
        dbt_vars=dbt_vars,
        table_id=table_id,
        raw_dataset_id=dataset_id,
        raw_table_id=raw_table_id,
        mode=MODE,
    )

    # Rename flow run

    flow_name_prefix = coalesce_task([table_id, dataset_id])

    flow_name_now_time = coalesce_task([date_var, get_now_time()])

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=default_materialization_flow.name + " " + flow_name_prefix + ": ",
        now_time=flow_name_now_time,
    )

    dbt_client = get_k8s_dbt_client(mode=MODE, wait=rename_flow_run)

    RUNS = run_dbt_model.map(
        dbt_client=unmapped(dbt_client),
        dataset_id=unmapped(dataset_id),
        table_id=unmapped(table_id),
        _vars=_vars,
        dbt_alias=unmapped(dbt_alias),
        upstream=unmapped(upstream),
        downstream=unmapped(downstream),
        exclude=unmapped(exclude),
        flags=unmapped(flags),
    )

    with case(flag_date_range, True):
        set_last_run_timestamp(
            dataset_id=dataset_id,
            table_id=table_id,
            timestamp=date_var["date_range_end"],
            wait=RUNS,
            mode=MODE,
        )


default_materialization_flow.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
default_materialization_flow.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
