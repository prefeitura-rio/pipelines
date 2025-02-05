# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_brt_gps
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client
from pipelines.utils.tasks import (
    get_now_time,
    rename_current_flow_run_now_time,
    get_current_flow_mode,
    get_current_flow_labels,
)

# SMTR Imports #

from pipelines.rj_smtr.constants import constants

# from pipelines.rj_smtr.schedules import (
#     every_minute,
#     every_hour,
# )
from pipelines.rj_smtr.tasks import (
    create_date_hour_partition,
    create_local_partition_path,
    fetch_dataset_sha,
    get_current_timestamp,
    get_materialization_date_range,
    # get_local_dbt_client,
    get_raw,
    parse_timestamp_to_string,
    save_raw_local,
    save_treated_local,
    set_last_run_timestamp,
    upload_logs_to_bq,
    bq_upload,
)
from pipelines.utils.execute_dbt_model.tasks import run_dbt_model

from pipelines.rj_smtr.br_rj_riodejaneiro_brt_gps.tasks import (
    pre_treatment_br_rj_riodejaneiro_brt_gps,
)

# Flows #

with Flow(
    "SMTR: GPS BRT - Materialização",
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as materialize_brt:
    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix=materialize_brt.name + ": ", now_time=get_now_time()
    )

    # Get default parameters #
    raw_dataset_id = Parameter(
        "raw_dataset_id", default=constants.GPS_BRT_RAW_DATASET_ID.value
    )
    raw_table_id = Parameter(
        "raw_table_id", default=constants.GPS_BRT_RAW_TABLE_ID.value
    )
    dataset_id = Parameter("dataset_id", default=constants.GPS_BRT_DATASET_ID.value)
    table_id = Parameter("table_id", default=constants.GPS_BRT_TREATED_TABLE_ID.value)
    rebuild = Parameter("rebuild", False)

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    # Set dbt client #
    dbt_client = get_k8s_dbt_client(mode=MODE, wait=rename_flow_run)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    # Set specific run parameters #
    date_range = get_materialization_date_range(
        dataset_id=dataset_id,
        table_id=table_id,
        raw_dataset_id=raw_dataset_id,
        raw_table_id=raw_table_id,
        table_run_datetime_column_name="timestamp_gps",
        mode=MODE,
        delay_hours=constants.GPS_BRT_MATERIALIZE_DELAY_HOURS.value,
    )
    dataset_sha = fetch_dataset_sha(
        dataset_id=dataset_id,
    )

    # Run materialization #
    with case(rebuild, True):
        RUN = run_dbt_model(
            dbt_client=dbt_client,
            dataset_id=dataset_id,
            table_id=table_id,
            upstream=True,
            exclude="+data_versao_efetiva",
            _vars=[date_range, dataset_sha],
            flags="--full-refresh",
        )
        set_last_run_timestamp(
            dataset_id=dataset_id,
            table_id=table_id,
            timestamp=date_range["date_range_end"],
            wait=RUN,
            mode=MODE,
        )
    with case(rebuild, False):
        RUN = run_dbt_model(
            dbt_client=dbt_client,
            dataset_id=dataset_id,
            table_id=table_id,
            upstream=True,
            exclude="+data_versao_efetiva",
            _vars=[date_range, dataset_sha],
        )
        set_last_run_timestamp(
            dataset_id=dataset_id,
            table_id=table_id,
            timestamp=date_range["date_range_end"],
            wait=RUN,
            mode=MODE,
        )

materialize_brt.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
materialize_brt.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
# materialize_brt.schedule = every_hour


with Flow(
    "SMTR: GPS BRT - Captura",
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as captura_brt:
    timestamp = get_current_timestamp()

    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix=captura_brt.name + ": ", now_time=timestamp
    )

    # SETUP LOCAL #
    partitions = create_date_hour_partition(timestamp)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=constants.GPS_BRT_RAW_DATASET_ID.value,
        table_id=constants.GPS_BRT_RAW_TABLE_ID.value,
        filename=filename,
        partitions=partitions,
    )
    # EXTRACT

    raw_status = get_raw(
        url=constants.GPS_BRT_API_URL.value,
        headers=constants.GPS_BRT_API_SECRET_PATH.value,
    )

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)
    # TREAT
    treated_status = pre_treatment_br_rj_riodejaneiro_brt_gps(
        status=raw_status, timestamp=timestamp
    )

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)
    # LOAD
    error = bq_upload(
        dataset_id=constants.GPS_BRT_RAW_DATASET_ID.value,
        table_id=constants.GPS_BRT_RAW_TABLE_ID.value,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )
    upload_logs_to_bq(
        dataset_id=constants.GPS_BRT_RAW_DATASET_ID.value,
        parent_table_id=constants.GPS_BRT_RAW_TABLE_ID.value,
        timestamp=timestamp,
        error=error,
    )
    captura_brt.set_dependencies(task=partitions, upstream_tasks=[rename_flow_run])

captura_brt.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_brt.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
# captura_brt.schedule = every_minute
