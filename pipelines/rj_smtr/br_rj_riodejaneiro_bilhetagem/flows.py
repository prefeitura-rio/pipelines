# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_bilhetagem
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import rename_current_flow_run_now_time

# SMTR Imports #

from pipelines.rj_smtr.constants import constants

from pipelines.rj_smtr.tasks import (
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

from pipelines.rj_smtr.schedules import (
    # every_minute,
    every_10_minutes,
)

from pipelines.rj_smtr.br_rj_riodejaneiro_bilhetagem.tasks import (
    get_bilhetagem_params,
    get_bilhetagem_url,
    get_datetime_range,
)

# Flows #

BILHETAGEM_FLOW_NAME = "[Teste] SMTR: Bilhetagem (captura)"

with Flow(
    BILHETAGEM_FLOW_NAME,
    code_owners=["rodrigo"],  # ["caio", "fernanda", "boris", "rodrigo"],
) as bilhetagem_captura:
    # SETUP #

    # Get default parameters #
    dataset_id = constants.BILHETAGEM_DATASET_ID.value
    table_id = constants.BILHETAGEM_TABLE_ID.value

    # SETUP
    timestamp = get_current_timestamp()

    datetime_range = get_datetime_range(timestamp)

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=BILHETAGEM_FLOW_NAME + ": ", now_time=timestamp
    )

    partitions = create_date_hour_partition(timestamp)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=dataset_id,
        table_id=table_id,
        filename=filename,
        partitions=partitions,
    )

    # EXTRACT #
    base_params, params, url = get_bilhetagem_url(datetime_range)

    count_rows = get_raw(
        url=url,
        headers=constants.BILHETAGEM_SECRET_PATH.value,
        base_params=base_params,
        params=params,
    )

    params = get_bilhetagem_params(count_rows, datetime_range)

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

bilhetagem_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
bilhetagem_captura.schedule = every_10_minutes
