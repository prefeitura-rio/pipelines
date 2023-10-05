# -*- coding: utf-8 -*-
"""
Flows for rj_smtr
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect import case, Parameter
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
    create_date_hour_partition,
    create_local_partition_path,
    get_current_timestamp,
    parse_timestamp_to_string,
    transform_raw_to_nested_structure,
    create_dbt_run_vars,
    set_last_run_timestamp,
    coalesce_task,
    upload_raw_data_to_gcs,
    upload_staging_data_to_gcs,
    get_raw_from_sources,
    create_request_params,
)

from pipelines.utils.execute_dbt_model.tasks import run_dbt_model

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

    timestamp = get_current_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=default_capture_flow.name + " " + table_id + ": ",
        now_time=timestamp,
    )

    partitions = create_date_hour_partition(
        timestamp, partition_date_only=partition_date_only
    )

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=dataset_id,
        table_id=table_id,
        filename=filename,
        partitions=partitions,
    )

    # Extração #
    request_params, request_path = create_request_params(
        dataset_id=dataset_id,
        extract_params=extract_params,
        table_id=table_id,
        timestamp=timestamp,
    )

    error, raw_filepath = get_raw_from_sources(
        source_type=source_type,
        local_filepath=filepath,
        source_path=request_path,
        dataset_id=dataset_id,
        table_id=table_id,
        secret_path=secret_path,
        request_params=request_params,
    )

    error = upload_raw_data_to_gcs(
        error=error,
        raw_filepath=raw_filepath,
        table_id=table_id,
        dataset_id=dataset_id,
        partitions=partitions,
    )

    # Pré-tratamento #

    error, staging_filepath = transform_raw_to_nested_structure(
        raw_filepath=raw_filepath,
        filepath=filepath,
        error=error,
        timestamp=timestamp,
        primary_key=primary_key,
    )

    STAGING_UPLOADED = upload_staging_data_to_gcs(
        error=error,
        staging_filepath=staging_filepath,
        timestamp=timestamp,
        table_id=table_id,
        dataset_id=dataset_id,
        partitions=partitions,
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
