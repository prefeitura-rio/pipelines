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

# from pipelines.rj_smtr.schedules import (
#     every_day_hour_seven,
# )
from pipelines.rj_smtr.tasks import (
    create_date_hour_partition,
    create_local_partition_path,
    get_current_timestamp,
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
    get_raw_ftp,
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
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as sppo_licenciamento_captura:
    timestamp = Parameter("timestamp", default=None)
    get_from_storage = Parameter("get_from_storage", default=False)

    timestamp = get_current_timestamp(timestamp=timestamp)

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix=f"{sppo_licenciamento_captura.name} - ", now_time=timestamp
    )

    # SETUP #
    partitions = create_date_hour_partition(timestamp, partition_date_only=True)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=constants.VEICULO_DATASET_ID.value,
        table_id=constants.SPPO_LICENCIAMENTO_TABLE_ID.value,
        filename=filename,
        partitions=partitions,
    )

    # EXTRACT
    raw_status_gcs = get_veiculo_raw_storage(
        dataset_id=constants.VEICULO_DATASET_ID.value,
        table_id=constants.SPPO_LICENCIAMENTO_TABLE_ID.value,
        timestamp=timestamp,
        csv_args=constants.SPPO_LICENCIAMENTO_CSV_ARGS.value,
    )

    raw_status_url = get_raw_ftp(
        ftp_path="LICENCIAMENTO/CadastrodeVeiculos",
        filetype="txt",
        csv_args=constants.SPPO_LICENCIAMENTO_CSV_ARGS.value,
        timestamp=timestamp,
    )

    ifelse(get_from_storage.is_equal(True), raw_status_gcs, raw_status_url)

    raw_status = merge(raw_status_gcs, raw_status_url)

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

    # TREAT
    treated_status = pre_treatment_sppo_licenciamento(
        status=raw_status, timestamp=timestamp
    )

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD
    error = bq_upload(
        dataset_id=constants.VEICULO_DATASET_ID.value,
        table_id=constants.SPPO_LICENCIAMENTO_TABLE_ID.value,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )
    upload_logs_to_bq(
        dataset_id=constants.VEICULO_DATASET_ID.value,
        parent_table_id=constants.SPPO_LICENCIAMENTO_TABLE_ID.value,
        timestamp=timestamp,
        error=error,
    )
    sppo_licenciamento_captura.set_dependencies(
        task=partitions, upstream_tasks=[rename_flow_run]
    )

sppo_licenciamento_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
sppo_licenciamento_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
# sppo_licenciamento_captura.schedule = every_day_hour_seven

with Flow(
    f"SMTR: {constants.VEICULO_DATASET_ID.value} {constants.SPPO_INFRACAO_TABLE_ID.value} - Captura",
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as sppo_infracao_captura:
    timestamp = Parameter("timestamp", default=None)
    get_from_storage = Parameter("get_from_storage", default=False)

    timestamp = get_current_timestamp(timestamp=timestamp)

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix=f"{sppo_infracao_captura.name} - ", now_time=timestamp
    )

    # SETUP #
    partitions = create_date_hour_partition(timestamp, partition_date_only=True)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=constants.VEICULO_DATASET_ID.value,
        table_id=constants.SPPO_INFRACAO_TABLE_ID.value,
        filename=filename,
        partitions=partitions,
    )

    # EXTRACT
    raw_status_gcs = get_veiculo_raw_storage(
        dataset_id=constants.VEICULO_DATASET_ID.value,
        table_id=constants.SPPO_INFRACAO_TABLE_ID.value,
        timestamp=timestamp,
        csv_args=constants.SPPO_INFRACAO_CSV_ARGS.value,
    )
    raw_status_url = get_raw_ftp(
        ftp_path="MULTAS/MULTAS",
        filetype="txt",
        csv_args=constants.SPPO_INFRACAO_CSV_ARGS.value,
        timestamp=timestamp,
    )
    ifelse(get_from_storage.is_equal(True), raw_status_gcs, raw_status_url)

    raw_status = merge(raw_status_gcs, raw_status_url)

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

    # TREAT
    treated_status = pre_treatment_sppo_infracao(status=raw_status, timestamp=timestamp)

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD
    error = bq_upload(
        dataset_id=constants.VEICULO_DATASET_ID.value,
        table_id=constants.SPPO_INFRACAO_TABLE_ID.value,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )
    upload_logs_to_bq(
        dataset_id=constants.VEICULO_DATASET_ID.value,
        parent_table_id=constants.SPPO_INFRACAO_TABLE_ID.value,
        timestamp=timestamp,
        error=error,
    )
    sppo_infracao_captura.set_dependencies(
        task=partitions, upstream_tasks=[rename_flow_run]
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
# veiculo_sppo_registro_agente_verao_captura.schedule = every_day_hour_seven
