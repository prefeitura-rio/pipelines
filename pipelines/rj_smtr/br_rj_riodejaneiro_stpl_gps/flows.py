# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_stpl_gps
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import rename_current_flow_run_now_time
from pipelines.rj_smtr.br_rj_riodejaneiro_stpl_gps.tasks import get_stpl_headers

# SMTR Imports #

from pipelines.rj_smtr.constants import constants

from pipelines.rj_smtr.schedules import (
    every_minute,
    # every_hour,
)
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
)

from pipelines.rj_smtr.br_rj_riodejaneiro_stpl_gps.tasks import (
    pre_treatment_br_rj_riodejaneiro_stpl_gps,
)


with Flow(
    "SMTR: GPS STPL - Captura",
    code_owners=["caio", "fernanda"],
) as captura_stpl:

    # DEFAULT PARAMETERS #
    dataset_id = Parameter(
        "dataset_id", default=constants.GPS_STPL_RAW_DATASET_ID.value
    )
    table_id = Parameter("table_id", default=constants.GPS_STPL_RAW_TABLE_ID.value)
    url = Parameter("url", default=constants.GPS_STPL_API_BASE_URL.value)
    secret_path = Parameter(
        "secret_path", default=constants.GPS_STPL_API_SECRET_PATH.value
    )

    # SETUP #
    timestamp = get_current_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix="SMTR: GPS STPL - Captura - ", now_time=timestamp
    )

    partitions = create_date_hour_partition(timestamp)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=constants.GPS_STPL_RAW_DATASET_ID.value,
        table_id=constants.GPS_STPL_RAW_TABLE_ID.value,
        filename=filename,
        partitions=partitions,
    )

    raw_status = get_raw(url=url, headers=get_stpl_headers())

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

    # TREAT
    treated_status = pre_treatment_br_rj_riodejaneiro_stpl_gps(
        status_dict=raw_status, timestamp=timestamp
    )

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD
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
        timestamp=timestamp,
        error=error,
    )
    # FLOW
    # ? Congelado para evitar o TriggerFailed, permitindo testar as demais tasks
    captura_stpl.set_dependencies(task=partitions, upstream_tasks=[rename_flow_run])


captura_stpl.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_stpl.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
# Seguindo o padrão de captura adotado pelo BRT
captura_stpl.schedule = every_minute
