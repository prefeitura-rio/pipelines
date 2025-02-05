# -*- coding: utf-8 -*-
# pylint: disable=W0511
"""
Flows for veiculos
"""


from copy import deepcopy
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect import Parameter

# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
    get_now_time,
    get_current_flow_labels,
    get_current_flow_mode,
)
from pipelines.utils.utils import set_default_parameters

# SMTR Imports #

from pipelines.rj_smtr.flows import (
    default_capture_flow,
)
from pipelines.rj_smtr.tasks import (
    get_current_timestamp,
    create_date_hour_partition,
    parse_timestamp_to_string,
    create_local_partition_path,
    upload_raw_data_to_gcs,
    transform_raw_to_nested_structure,
    upload_staging_data_to_gcs,
)

from pipelines.rj_smtr.constants import constants as smtr_constants

from pipelines.rj_smtr.schedules import every_day, every_friday_seven_thirty

from pipelines.rj_smtr.controle_financeiro.constants import constants
from pipelines.rj_smtr.controle_financeiro.tasks import (
    get_cct_arquivo_retorno_redis_key,
    create_cct_arquivo_retorno_params,
    get_raw_cct_arquivo_retorno,
    cct_arquivo_retorno_save_redis,
)


# Flows #

controle_cct_cb_captura = deepcopy(default_capture_flow)
controle_cct_cb_captura.name = "SMTR: Controle Financeiro CB - Captura"
controle_cct_cb_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
controle_cct_cb_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

controle_cct_cb_captura = set_default_parameters(
    flow=controle_cct_cb_captura,
    default_parameters=constants.SHEETS_CAPTURE_DEFAULT_PARAMS.value
    | constants.SHEETS_CB_CAPTURE_PARAMS.value,
)
# controle_cct_cb_captura.schedule = every_day

controle_cct_cett_captura = deepcopy(default_capture_flow)
controle_cct_cett_captura.name = "SMTR: Controle Financeiro CETT - Captura"
controle_cct_cett_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
controle_cct_cett_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

controle_cct_cett_captura = set_default_parameters(
    flow=controle_cct_cett_captura,
    default_parameters=constants.SHEETS_CAPTURE_DEFAULT_PARAMS.value
    | constants.SHEETS_CETT_CAPTURE_PARAMS.value,
)
# controle_cct_cett_captura.schedule = every_day


with Flow(
    "SMTR: Controle Financeiro Arquivo Retorno - Captura",
    code_owners=["caio", "fernanda", "boris", "rodrigo", "rafaelpinheiro"],
) as arquivo_retorno_captura:
    start_date = Parameter("start_date", default=None)
    end_date = Parameter("end_date", default=None)

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=f"SMTR: Captura {constants.ARQUIVO_RETORNO_TABLE_ID.value}: ",
        now_time=get_now_time(),
    )
    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    timestamp = get_current_timestamp()

    REDIS_KEY = get_cct_arquivo_retorno_redis_key(mode=MODE)

    headers, params = create_cct_arquivo_retorno_params(
        redis_key=REDIS_KEY,
        start_date=start_date,
        end_date=end_date,
    )

    partitions = create_date_hour_partition(
        timestamp,
        partition_date_only=True,
    )

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=smtr_constants.CONTROLE_FINANCEIRO_DATASET_ID.value,
        table_id=constants.ARQUIVO_RETORNO_TABLE_ID.value,
        filename=filename,
        partitions=partitions,
    )

    raw_filepath = get_raw_cct_arquivo_retorno(
        headers=headers,
        params=params,
        local_filepath=filepath,
    )

    error = upload_raw_data_to_gcs(
        error=None,
        raw_filepath=raw_filepath,
        table_id=constants.ARQUIVO_RETORNO_TABLE_ID.value,
        dataset_id=smtr_constants.CONTROLE_FINANCEIRO_DATASET_ID.value,
        partitions=partitions,
    )

    error, staging_filepath = transform_raw_to_nested_structure(
        raw_filepath=raw_filepath,
        filepath=filepath,
        error=error,
        timestamp=timestamp,
        primary_key=["id"],
    )

    staging_upload = upload_staging_data_to_gcs(
        error=error,
        staging_filepath=staging_filepath,
        timestamp=timestamp,
        table_id=constants.ARQUIVO_RETORNO_TABLE_ID.value,
        dataset_id=smtr_constants.CONTROLE_FINANCEIRO_DATASET_ID.value,
        partitions=partitions,
    )

    cct_arquivo_retorno_save_redis(
        redis_key=REDIS_KEY,
        raw_filepath=raw_filepath,
        upstream_tasks=[staging_upload],
    )

arquivo_retorno_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
arquivo_retorno_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

# arquivo_retorno_captura.schedule = every_friday_seven_thirty
