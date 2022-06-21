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
from pipelines.utils.tasks import rename_current_flow_run_now_time, get_now_time

# SMTR Imports #

from pipelines.rj_smtr.constants import constants

# from pipelines.rj_smtr.schedules import every_minute
from pipelines.rj_smtr.tasks import (
    create_current_date_hour_partition,
    create_local_partition_path,
    get_raw,
    save_raw_local,
    save_treated_local,
    upload_logs_to_bq,
    bq_upload,
)

from pipelines.rj_smtr.br_rj_riodejaneiro_stpl_gps.tasks import (
    pre_treatment_br_rj_riodejaneiro_stpl_gps,
)


# Flows #

with Flow(
    "SMTR: GPS STPL - Captura",
    code_owners=["caio", "fernanda"],
) as stpl_captura:

    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix="GPS BRT - Materialização: ", now_time=get_now_time()
    )

    # Get default parameters #
    dataset_id = Parameter("dataset_id", default=constants.GPS_STPL_DATASET_ID.value)
    table_id = Parameter("table_id", default=constants.GPS_STPL_RAW_TABLE_ID.value)
    url = Parameter("url", default=constants.GPS_STPL_API_BASE_URL.value)
    secret_path = Parameter(
        "secret_path", default=constants.GPS_STPL_API_SECRET_PATH.value
    )

    file_dict = create_current_date_hour_partition()

    filepath = create_local_partition_path(
        dataset_id=dataset_id,
        table_id=table_id,
        filename=file_dict["filename"],
        partitions=file_dict["partitions"],
    )

    status_dict = get_raw(url=url, source=secret_path)

    raw_filepath = save_raw_local(data=status_dict["data"], file_path=filepath)

    treated_status = pre_treatment_br_rj_riodejaneiro_stpl_gps(status_dict=status_dict)

    upload_logs_to_bq(
        dataset_id=dataset_id,
        parent_table_id=table_id,
        timestamp=status_dict["timestamp"],
        error=status_dict["error"],
    )

    treated_filepath = save_treated_local(
        dataframe=treated_status["df"], file_path=filepath
    )

    bq_upload(
        dataset_id=dataset_id,
        table_id=table_id,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=file_dict["partitions"],
    )
    stpl_captura.set_dependencies(task=file_dict, upstream_tasks=[rename_flow_run])

stpl_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
stpl_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
# stpl_captura.schedule = every_minute
