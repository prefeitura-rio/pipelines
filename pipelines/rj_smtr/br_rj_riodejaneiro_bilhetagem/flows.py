# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_bilhetagem
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped
from prefect import case, Parameter
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
    get_current_flow_labels,
    get_current_flow_mode,
)

# SMTR Imports #

from pipelines.rj_smtr.constants import constants

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
    pre_treatment_nested_data,
    get_project_name,
)

from pipelines.rj_smtr.schedules import every_minute, every_day

from pipelines.rj_smtr.br_rj_riodejaneiro_bilhetagem.tasks import (
    get_bilhetagem_params,
    get_datetime_range,
    generate_bilhetagem_flow_params,
)

# Flows #

BILHETAGEM_TRANSACAO_FLOW_NAME = "SMTR: Bilhetagem Transação (captura)"

with Flow(
    BILHETAGEM_TRANSACAO_FLOW_NAME,
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as bilhetagem_transacao_captura:
    # SETUP #

    DATASET_ID = constants.BILHETAGEM_DATASET_ID.value
    TABLE_ID = constants.BILHETAGEM_TRANSACAO_TABLE_ID.value
    timestamp_param = Parameter("timestamp", default=None)
    interval_minutes_param = Parameter("interval_minutes", default=1)

    timestamp = get_current_timestamp(timestamp_param)

    datetime_range = get_datetime_range(
        timestamp, interval_minutes=interval_minutes_param
    )

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=BILHETAGEM_TRANSACAO_FLOW_NAME + ": ", now_time=timestamp
    )

    # EXTRACT #
    base_params, params, url = get_bilhetagem_params(datetime_range)

    partitions = create_date_hour_partition(timestamp)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        filename=filename,
        partitions=partitions,
    )

    raw_status = get_raw(
        url=url,
        headers=constants.BILHETAGEM_SECRET_PATH.value,
        base_params=base_params,
        params=params,
    )

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

    # TREAT & CLEAN #
    treated_status = pre_treatment_nested_data(
        status=raw_status, timestamp=timestamp, primary_key=["id"]
    )

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD #
    error = bq_upload(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )

    upload_logs_to_bq(
        dataset_id=DATASET_ID,
        parent_table_id=TABLE_ID,
        error=error,
        timestamp=timestamp,
    )

bilhetagem_transacao_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_transacao_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
bilhetagem_transacao_captura.schedule = every_minute

BILHETAGEM_PRINCIPAL_FLOW_NAME = "SMTR: Bilhetagem Principal Auxiliar (captura)"

with Flow(
    BILHETAGEM_PRINCIPAL_FLOW_NAME,
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as bilhetagem_principal_captura:
    # SETUP #

    tables_params = Parameter("tables_params")
    datetime_range = Parameter("datetime_range")
    timestamp_param = Parameter("timestamp")

    timestamp = get_current_timestamp(timestamp_param)
    DATASET_ID = constants.BILHETAGEM_DATASET_ID.value

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=BILHETAGEM_PRINCIPAL_FLOW_NAME + " " + tables_params["table_id"] + ": ",
        now_time=timestamp,
    )

    base_params, params, url = get_bilhetagem_params(
        datetime_range=datetime_range,
        database=tables_params["database"],
        table_name=tables_params["table_name"],
        table_column=tables_params["table_column"],
        method=tables_params["method"],
    )

    partitions = create_date_partition(timestamp)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=DATASET_ID,
        table_id=tables_params["table_id"],
        filename=filename,
        partitions=partitions,
    )

    raw_status = get_raw(
        url=url,
        headers=constants.BILHETAGEM_SECRET_PATH.value,
        base_params=base_params,
        params=params,
    )

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

    # TREAT & CLEAN #
    treated_status = pre_treatment_nested_data(
        status=raw_status,
        timestamp=timestamp,
        primary_key=tables_params["primary_key"],
    )

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD #
    error = bq_upload(
        dataset_id=DATASET_ID,
        table_id=tables_params["table_id"],
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )

    upload_logs_to_bq(
        dataset_id=DATASET_ID,
        parent_table_id=tables_params["table_id"],
        error=error,
        timestamp=timestamp,
    )

bilhetagem_principal_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_principal_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

BILHETAGEM_PRINCIPAL_MAIN_FLOW_NAME = "SMTR: Bilhetagem Principal Geral (captura)"

with Flow(
    BILHETAGEM_PRINCIPAL_MAIN_FLOW_NAME,
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as bilhetagem_principal_geral_captura:
    # SETUP #

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)
    PROJECT_NAME = get_project_name(MODE)

    DATASET_ID = constants.BILHETAGEM_DATASET_ID.value
    tables_params = Parameter(
        "tables_params",
        default=constants.BILHETAGEM_PRINCIPAL_TRANSACAO_TABLES_PARAMS.value,
    )

    timestamp = get_current_timestamp()

    datetime_range = get_datetime_range(timestamp=timestamp, interval_minutes=1440)

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=BILHETAGEM_PRINCIPAL_MAIN_FLOW_NAME + ": ", now_time=timestamp
    )

    # EXTRACT #
    flow_params = generate_bilhetagem_flow_params(
        timestamp=timestamp,
        datetime_range=datetime_range,
        tables_params=tables_params,
    )

    BILHETAGEM_PRINCIPAL_CAPTURA_RUN = create_flow_run.map(
        flow_name=unmapped(BILHETAGEM_PRINCIPAL_FLOW_NAME),
        project_name=unmapped(PROJECT_NAME),
        run_name=unmapped(BILHETAGEM_PRINCIPAL_FLOW_NAME),
        parameters=flow_params,
    )

    wait_for_flow_run.map(
        BILHETAGEM_PRINCIPAL_CAPTURA_RUN,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

bilhetagem_principal_geral_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_principal_geral_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
bilhetagem_principal_geral_captura.schedule = every_day
