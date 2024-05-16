# -*- coding: utf-8 -*-
# pylint: disable=W0511
"""
Flows for veiculos
"""

from copy import deepcopy
from prefect import Parameter
from prefect.tasks.control_flow import ifelse, merge
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

# EMD Imports #

from pipelines.constants import constants as emd_constants

from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client
from pipelines.utils.utils import set_default_parameters

from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
    get_current_flow_mode,
    get_current_flow_labels,
)

# SMTR Imports #

from pipelines.rj_smtr.constants import constants

from pipelines.rj_smtr.schedules import (
    every_day_hour_seven,
)
from pipelines.rj_smtr.tasks import (
    create_date_hour_partition,
    create_local_partition_path,
    get_current_timestamp,
    get_raw,
    get_rounded_timestamp,
    parse_timestamp_to_string,
    save_raw_local,
    save_treated_local,
    upload_logs_to_bq,
    bq_upload,
    fetch_dataset_sha,
    get_run_dates,
    get_join_dict,
    get_previous_date,
)

from pipelines.rj_smtr.veiculo.tasks import (
    download_and_save_local_from_ftp,
    get_ftp_filepaths,
    pre_treatment_sppo_licenciamento,
    pre_treatment_sppo_infracao,
    get_veiculo_raw_storage,
)

from pipelines.utils.execute_dbt_model.tasks import run_dbt_model

from pipelines.rj_smtr.flows import default_capture_flow

# Flows #

# flake8: noqa: E501
with Flow(
    f"SMTR: {constants.VEICULO_DATASET_ID.value} {constants.SPPO_LICENCIAMENTO_TABLE_ID.value} - Captura",
    # code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as sppo_licenciamento_captura:

    timestamp = Parameter("timestamp", default=None)
    search_dir = Parameter("search_dir", default="licenciamento")
    dataset_id = Parameter("dataset_id", default=constants.VEICULO_DATASET_ID.value)
    table_id = Parameter(
        "table_id", default=constants.SPPO_LICENCIAMENTO_TABLE_ID.value
    )

    timestamp = get_rounded_timestamp(timestamp)
    # EXTRACT
    files = get_ftp_filepaths(search_dir=search_dir, timestamp=timestamp)
    updated_files_info = download_and_save_local_from_ftp.map(
        file_info=files, dataset_id=dataset_id, table_id=table_id
    )
    # TRANSFORM
    treated_paths, raw_paths, partitions, status = pre_treatment_sppo_licenciamento(
        files=updated_files_info
    )

    # LOAD
    errors = bq_upload.map(
        dataset_id=unmapped(dataset_id),
        table_id=unmapped(table_id),
        filepath=treated_paths,
        raw_filepath=raw_paths,
        partitions=partitions,
        status=status,
    )

sppo_licenciamento_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
sppo_licenciamento_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
# sppo_licenciamento_captura.schedule = every_day_hour_seven

with Flow(
    f"SMTR: {constants.VEICULO_DATASET_ID.value} {constants.SPPO_INFRACAO_TABLE_ID.value} - Captura",
    # code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as sppo_infracao_captura:
    timestamp = Parameter("timestamp", default=None)
    search_dir = Parameter("search_dir", default="multas")
    dataset_id = Parameter("dataset_id", default=constants.VEICULO_DATASET_ID.value)
    table_id = Parameter("table_id", default=constants.SPPO_INFRACAO_TABLE_ID.value)

    # MODE = get_current_flow_mode()
    timestamp = get_rounded_timestamp(timestamp)
    # EXTRACT
    files = get_ftp_filepaths(search_dir=search_dir, timestamp=timestamp)
    updated_files_info = download_and_save_local_from_ftp.map(
        file_info=files, dataset_id=dataset_id, table_id=table_id
    )
    # TRANSFORM
    treated_paths, raw_paths, partitions, status = pre_treatment_sppo_infracao(
        files=updated_files_info
    )

    # LOAD
    errors = bq_upload.map(
        dataset_id=unmapped(dataset_id),
        table_id=unmapped(table_id),
        filepath=treated_paths,
        raw_filepath=raw_paths,
        partitions=partitions,
        status=status,
    )

sppo_infracao_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
sppo_infracao_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
# sppo_infracao_captura.schedule = every_day_hour_seven

# flake8: noqa: E501
with Flow(
    f"SMTR: {constants.VEICULO_DATASET_ID.value} {constants.SPPO_VEICULO_DIA_TABLE_ID.value} - Materialização (subflow)",
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as sppo_veiculo_dia:
    # 1. SETUP #

    # Get default parameters #
    start_date = Parameter("start_date", default=get_previous_date.run(1))
    end_date = Parameter("end_date", default=get_previous_date.run(1))
    stu_data_versao = Parameter("stu_data_versao", default="")

    run_dates = get_run_dates(start_date, end_date)

    # Rename flow run #
    rename_flow_run = rename_current_flow_run_now_time(
        prefix=sppo_veiculo_dia.name + ": ",
        now_time=run_dates,
    )

    # Set dbt client #
    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    dbt_client = get_k8s_dbt_client(mode=MODE, wait=rename_flow_run)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    # Get models version #
    # TODO: include version in a column in the table
    dataset_sha = fetch_dataset_sha(
        dataset_id=constants.VEICULO_DATASET_ID.value,
    )

    dict_list = get_join_dict(dict_list=run_dates, new_dict=dataset_sha)
    _vars = get_join_dict(
        dict_list=dict_list, new_dict={"stu_data_versao": stu_data_versao}
    )

    # 2. TREAT #
    run_dbt_model.map(
        dbt_client=unmapped(dbt_client),
        dataset_id=unmapped(constants.VEICULO_DATASET_ID.value),
        table_id=unmapped(constants.SPPO_VEICULO_DIA_TABLE_ID.value),
        upstream=unmapped(True),
        exclude=unmapped("+gps_sppo"),
        _vars=_vars,
    )

sppo_veiculo_dia.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
sppo_veiculo_dia.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

veiculo_sppo_registro_agente_verao_captura = deepcopy(default_capture_flow)
veiculo_sppo_registro_agente_verao_captura.name = f"SMTR: {constants.VEICULO_DATASET_ID.value} {constants.SPPO_REGISTRO_AGENTE_VERAO_PARAMS.value['table_id']} - Captura"
veiculo_sppo_registro_agente_verao_captura.storage = GCS(
    emd_constants.GCS_FLOWS_BUCKET.value
)
veiculo_sppo_registro_agente_verao_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
veiculo_sppo_registro_agente_verao_captura.storage = GCS(
    emd_constants.GCS_FLOWS_BUCKET.value
)
veiculo_sppo_registro_agente_verao_captura = set_default_parameters(
    flow=veiculo_sppo_registro_agente_verao_captura,
    default_parameters=constants.SPPO_REGISTRO_AGENTE_VERAO_PARAMS.value,
)
veiculo_sppo_registro_agente_verao_captura.schedule = every_day_hour_seven
