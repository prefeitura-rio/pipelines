# -*- coding: utf-8 -*-
"""
Flows for operacao
"""

# from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.decorators import Flow

# from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
    get_current_flow_mode,
    get_current_flow_labels,
)

# SMTR Imports #

from pipelines.rj_smtr.operacao.constants import constants

from pipelines.rj_smtr.schedules import (
    every_day,
)
from pipelines.rj_smtr.tasks import (
    create_date_hour_partition,
    create_local_partition_path,
    # fetch_dataset_sha,
    get_current_timestamp,
    # get_local_dbt_client,
    get_raw,
    parse_timestamp_to_string,
    save_raw_local,
    save_treated_local,
    set_last_run_timestamp,
    upload_logs_to_bq,
    bq_upload,
)

# from pipelines.utils.execute_dbt_model.tasks import run_dbt_model

from pipelines.rj_smtr.operacao.tasks import (
    pre_treatment_sppo_infracao,
)

# Flows #

sppo_infracao_captura_name = f"SMTR: Captura - {constants.DATASET_ID.value}.{constants.SPPO_INFRACAO_TABLE_ID.value}"
with Flow(
    sppo_infracao_captura_name,
    code_owners=["rodrigo", "fernanda"],
) as sppo_infracao_captura:

    timestamp = get_current_timestamp()

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix=f"{sppo_infracao_captura_name} - ", now_time=timestamp
    )

    # SETUP #
    partitions = create_date_hour_partition(timestamp, date_only=True)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=constants.DATASET_ID.value,
        table_id=constants.SPPO_INFRACAO_TABLE_ID.value,
        filename=filename,
        partitions=partitions,
    )

    # EXTRACT
    # URL = "https://apps.data.rio/SMTR/Multas/multas.txt"

    # TODO: Alterar para link do FTP a ser definido # pylint: disable=W0511
    # flake8: noqa: E501
    URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vS47M-uRV-L5F67Qq26UVJJuSqpm1RPVexG4XsCM0IcTopPoPB3dkFbwZ2eoJrf6Ou9w7KMcSTfI2hy/pub?output=csv"

    raw_status = get_raw(
        url=URL,
        headers=constants.SPPO_INFRACAO_MAPPING_KEYS.value,
        filetype="txt",
    )

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath, filetype="txt")

    # TREAT
    treated_status = pre_treatment_sppo_infracao(status=raw_status, timestamp=timestamp)

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD
    error = bq_upload(
        dataset_id=constants.DATASET_ID.value,
        table_id=constants.SPPO_INFRACAO_TABLE_ID.value,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )
    upload_logs_to_bq(
        dataset_id=constants.DATASET_ID.value,
        parent_table_id=constants.SPPO_INFRACAO_TABLE_ID.value,
        timestamp=timestamp,
        error=error,
    )
    sppo_infracao_captura.set_dependencies(
        task=partitions, upstream_tasks=[rename_flow_run]
    )

    # REDIS SET LAST RUN
    set_last_run_timestamp(
        dataset_id=constants.DATASET_ID.value,
        table_id=constants.SPPO_INFRACAO_TABLE_ID.value,
        timestamp=timestamp,
        mode=MODE,
        wait=error,
    )

sppo_infracao_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
sppo_infracao_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
sppo_infracao_captura.schedule = every_day
