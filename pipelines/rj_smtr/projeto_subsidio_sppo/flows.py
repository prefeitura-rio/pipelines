# -*- coding: utf-8 -*-
"""
Flows for projeto_subsidio_sppo
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import merge

# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
)
from pipelines.utils.decorators import Flow

# from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client

# SMTR Imports #

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.tasks import (
    create_date_hour_partition,
    create_local_partition_path,
    get_current_timestamp,
    # get_local_dbt_client,
    parse_timestamp_to_string,
    save_raw_local,
    save_treated_local,
    upload_logs_to_bq,
    bq_upload,
)

# from pipelines.rj_smtr.schedules import (
#     every_day_hour_six,
# )

from pipelines.rj_smtr.projeto_subsidio_sppo.tasks import (
    get_raw,
    pre_treatment_subsidio_sppo_recursos,
)

# Flows #

with Flow(
    "SMTR - Subsídio SPPO - Recursos Captura", code_owners=["caio", "fernanda"]
) as subsidio_sppo_recursos:

    # Get run parameters #
    date_range_start = Parameter("date_range_start", default="2022-10-03 00:00:00")
    date_range_end = Parameter("date_range_end", default="2022-10-04 00:00:00")

    # SETUP #
    timestamp = get_current_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix="Subsídio SPPO - Recursos Captura: ", now_time=timestamp
    )

    partitions = create_date_hour_partition(timestamp)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=constants.SUBSIDIO_SPPO_DATASET_ID.value,
        table_id=constants.SUBSIDIO_SPPO_RECURSO_TABLE_ID.value,
        filename=filename,
        partitions=partitions,
    )

    # EXTRACT #
    raw_status = get_raw(date_range_start, date_range_end)

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

    # TREAT
    treated_status = pre_treatment_subsidio_sppo_recursos(
        status=raw_status, timestamp=timestamp
    )

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD
    error = bq_upload(
        dataset_id=constants.SUBSIDIO_SPPO_DATASET_ID.value,
        table_id=constants.SUBSIDIO_SPPO_RECURSO_TABLE_ID.value,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )

    upload_logs_to_bq(
        dataset_id=constants.SUBSIDIO_SPPO_DATASET_ID.value,
        parent_table_id=constants.SUBSIDIO_SPPO_RECURSO_TABLE_ID.value,
        error=error,
        timestamp=timestamp,
    )

subsidio_sppo_recursos.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
subsidio_sppo_recursos.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
# subsidio_sppo_recursos.schedule = every_day_hour_six
