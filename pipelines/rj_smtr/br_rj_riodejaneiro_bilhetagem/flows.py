# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_bilhetagem
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped
from prefect import case

# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import rename_current_flow_run_now_time

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
    pre_treatment_nest_data,
)

from pipelines.rj_smtr.schedules import every_minute, every_day

from pipelines.rj_smtr.br_rj_riodejaneiro_bilhetagem.tasks import (
    get_bilhetagem_params,
    get_bilhetagem_url,
    get_datetime_range,
)

# Flows #

BILHETAGEM_TRANSACAO_FLOW_NAME = "SMTR: Bilhetagem Transação (captura)"

with Flow(
    BILHETAGEM_TRANSACAO_FLOW_NAME,
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as bilhetagem_transacao_captura:
    # SETUP #

    dataset_id = constants.BILHETAGEM_DATASET_ID.value
    table_id = constants.BILHETAGEM_TRANSACAO_TABLE_ID.value

    timestamp = get_current_timestamp()

    datetime_range = get_datetime_range(timestamp)

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=BILHETAGEM_TRANSACAO_FLOW_NAME + ": ", now_time=timestamp
    )

    # EXTRACT #
    base_params, params, url = get_bilhetagem_url(datetime_range)

    count_rows = get_raw(
        url=url,
        headers=constants.BILHETAGEM_SECRET_PATH.value,
        base_params=base_params,
        params=params,
    )

    flag_get_data, params = get_bilhetagem_params(count_rows, datetime_range)

    with case(flag_get_data, True):
        partitions = create_date_hour_partition(timestamp)

        filename = parse_timestamp_to_string(timestamp)

        filepath = create_local_partition_path(
            dataset_id=dataset_id,
            table_id=table_id,
            filename=filename,
            partitions=partitions,
        )

        raw_status = get_raw.map(
            url=unmapped(url),
            headers=unmapped(constants.BILHETAGEM_SECRET_PATH.value),
            base_params=unmapped(base_params),
            params=params,
        )

        raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

        # TREAT & CLEAN #
        treated_status = pre_treatment_nest_data(
            status=raw_status, timestamp=timestamp, primary_key=["id"]
        )

        treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

        # LOAD #
        error = bq_upload(
            dataset_id=dataset_id,
            table_id=table_id,
            filepath=treated_filepath,
            raw_filepath=raw_filepath,
            partitions=partitions,
            status=treated_status,
        )

        upload_logs_to_bq(
            dataset_id=dataset_id,
            parent_table_id=table_id,
            error=error,
            timestamp=timestamp,
        )

bilhetagem_transacao_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_transacao_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
bilhetagem_transacao_captura.schedule = every_minute


BILHETAGEM_PRINCIPAL_FLOW_NAME = "SMTR: Bilhetagem Principal (captura)"

with Flow(
    BILHETAGEM_PRINCIPAL_FLOW_NAME,
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as bilhetagem_principal_captura:
    # SETUP #

    dataset_id = constants.BILHETAGEM_DATASET_ID.value
    # TODO: change to tables_id and get some tables
    # (the constant should be a dict with all needed data)
    table_id = constants.BILHETAGEM_PRINCIPAL_TRANSACAO_TABLES_ID.value

    timestamp = get_current_timestamp()

    datetime_range = get_datetime_range(timestamp=timestamp, interval_minutes=1440)

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=BILHETAGEM_PRINCIPAL_FLOW_NAME + ": ", now_time=timestamp
    )

    # EXTRACT #
    base_params, params, url = get_bilhetagem_url(
        datetime_range=datetime_range,
        database="principal_db",
        table_name="LINHA",
        table_column="DT_INCLUSAO",
        method=">=",
    )

    count_rows = get_raw(
        url=url,
        headers=constants.BILHETAGEM_SECRET_PATH.value,
        base_params=base_params,
        params=params,
    )

    flag_get_data, params = get_bilhetagem_params(
        count_rows=count_rows,
        datetime_range=datetime_range,
        table_name="LINHA",
        table_column="DT_INCLUSAO",
        method=">=",
    )

    with case(flag_get_data, True):
        partitions = create_date_partition(timestamp)

        filename = parse_timestamp_to_string(timestamp)

        filepath = create_local_partition_path(
            dataset_id=dataset_id,
            table_id=table_id,
            filename=filename,
            partitions=partitions,
        )

        raw_status = get_raw.map(
            url=unmapped(url),
            headers=unmapped(constants.BILHETAGEM_SECRET_PATH.value),
            base_params=unmapped(base_params),
            params=params,
        )

        raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

        # TREAT & CLEAN #
        treated_status = pre_treatment_nest_data(
            status=raw_status, timestamp=timestamp, primary_key=["CD_LINHA"]
        )

        treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

        # LOAD #
        error = bq_upload(
            dataset_id=dataset_id,
            table_id=table_id,
            filepath=treated_filepath,
            raw_filepath=raw_filepath,
            partitions=partitions,
            status=treated_status,
        )

        upload_logs_to_bq(
            dataset_id=dataset_id,
            parent_table_id=table_id,
            error=error,
            timestamp=timestamp,
        )

bilhetagem_principal_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_principal_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
bilhetagem_principal_captura.schedule = every_day
