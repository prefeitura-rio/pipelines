# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_rdo
"""

from prefect import Parameter, case

from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants as emd_constants
from pipelines.rj_smtr.br_rj_riodejaneiro_rdo.tasks import (
    get_file_paths_from_ftp,
    check_files_for_download,
    download_and_save_local_from_ftp,
    pre_treatment_br_rj_riodejaneiro_rdo,
    get_rdo_date_range,
    update_rdo_redis,
)
from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.tasks import (
    bq_upload,
    get_current_timestamp,
    set_last_run_timestamp,
)

# from pipelines.rj_smtr.schedules import every_day

# from pipelines.rj_smtr.br_rj_riodejaneiro_rdo.schedules import every_two_weeks
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client
from pipelines.utils.tasks import (
    get_now_time,
    rename_current_flow_run_now_time,
    get_current_flow_mode,
    get_current_flow_labels,
)
from pipelines.utils.execute_dbt_model.tasks import run_dbt_model

with Flow(
    "SMTR: SPPO RHO - Materialização (subflow)",
    code_owners=constants.DEFAULT_CODE_OWNERS.value,
) as sppo_rho_materialize:
    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix=sppo_rho_materialize.name + ": ", now_time=get_now_time()
    )

    # Get default parameters #
    dataset_id = Parameter("dataset_id", default=constants.RDO_DATASET_ID.value)
    table_id = Parameter("table_id", default=constants.SPPO_RHO_TABLE_ID.value)
    rebuild = Parameter("rebuild", False)

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    # Set dbt client #
    dbt_client = get_k8s_dbt_client(mode=MODE)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    # Set specific run parameters #
    date_range = get_rdo_date_range(dataset_id=dataset_id, table_id=table_id, mode=MODE)
    # Run materialization #
    with case(rebuild, True):
        RUN = run_dbt_model(
            dbt_client=dbt_client,
            dataset_id=dataset_id,
            table_id=table_id,
            upstream=True,
            _vars=[date_range],
            flags="--full-refresh",
        )
        set_last_run_timestamp(
            dataset_id=dataset_id,
            table_id=table_id,
            timestamp=date_range["date_range_end"],
            wait=RUN,
            mode=MODE,
        )
    with case(rebuild, False):
        RUN = run_dbt_model(
            dbt_client=dbt_client,
            dataset_id=dataset_id,
            table_id=table_id,
            _vars=[date_range],
        )
        set_last_run_timestamp(
            dataset_id=dataset_id,
            table_id=table_id,
            timestamp=date_range["date_range_end"],
            wait=RUN,
            mode=MODE,
        )

sppo_rho_materialize.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
sppo_rho_materialize.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

with Flow(
    "SMTR: RHO - Captura (subflow)",
    code_owners=constants.DEFAULT_CODE_OWNERS.value,
) as captura_sppo_rho:
    # SETUP
    transport_mode = Parameter("transport_mode", "SPPO")
    report_type = Parameter("report_type", "RHO")
    dump = Parameter("dump", False)
    table_id = Parameter("table_id", constants.SPPO_RHO_TABLE_ID.value)
    materialize = Parameter("materialize", False)

    rename_run = rename_current_flow_run_now_time(
        prefix=f"{captura_sppo_rho.name} FTP - {transport_mode.run()}-{report_type.run()} ",
        now_time=get_current_timestamp(),
        wait=None,
    )
    # EXTRACT
    files = get_file_paths_from_ftp(
        transport_mode=transport_mode, report_type=report_type, dump=dump
    )
    download_files = check_files_for_download(
        files=files, dataset_id=constants.RDO_DATASET_ID.value, table_id=table_id
    )
    updated_info = download_and_save_local_from_ftp.map(file_info=download_files)
    # TRANSFORM
    treated_path, raw_path, partitions, status = pre_treatment_br_rj_riodejaneiro_rdo(
        files=updated_info
    )
    # LOAD
    errors = bq_upload.map(
        dataset_id=unmapped(constants.RDO_DATASET_ID.value),
        table_id=unmapped(table_id),
        filepath=treated_path,
        raw_filepath=raw_path,
        partitions=partitions,
        status=status,
    )
    set_redis = update_rdo_redis(
        download_files=download_files, table_id=table_id, errors=errors
    )

captura_sppo_rho.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_sppo_rho.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

with Flow(
    "SMTR: RHO - Captura/Tratamento",
    code_owners=constants.DEFAULT_CODE_OWNERS.value,
) as rho_captura_tratamento:
    LABELS = get_current_flow_labels()

    run_captura = create_flow_run(
        flow_name=captura_sppo_rho.name,
        project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
        labels=LABELS,
    )

    wait_captura = wait_for_flow_run(
        run_captura,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

    run_materializacao = create_flow_run(
        flow_name=sppo_rho_materialize.name,
        project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
        labels=LABELS,
        upstream_tasks=[wait_captura],
    )

    wait_materializacao = wait_for_flow_run(
        run_materializacao,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

rho_captura_tratamento.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
rho_captura_tratamento.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
# rho_captura_tratamento.schedule = every_day

with Flow(
    "SMTR: RDO - Captura",
    code_owners=constants.DEFAULT_CODE_OWNERS.value,
) as captura_sppo_rdo:
    # SETUP
    transport_mode = Parameter("transport_mode", "SPPO")
    report_type = Parameter("report_type", "RDO")
    dump = Parameter("dump", False)
    table_id = Parameter("table_id", constants.SPPO_RDO_TABLE_ID.value)
    materialize = Parameter("materialize", False)

    rename_run = rename_current_flow_run_now_time(
        prefix=f"{captura_sppo_rdo.name} FTP - {transport_mode.run()}-{report_type.run()} ",
        now_time=get_current_timestamp(),
        wait=None,
    )
    # EXTRACT
    files = get_file_paths_from_ftp(
        transport_mode=transport_mode, report_type=report_type, dump=dump
    )
    download_files = check_files_for_download(
        files=files, dataset_id=constants.RDO_DATASET_ID.value, table_id=table_id
    )
    updated_info = download_and_save_local_from_ftp.map(file_info=download_files)
    # TRANSFORM
    treated_path, raw_path, partitions, status = pre_treatment_br_rj_riodejaneiro_rdo(
        files=updated_info
    )
    # LOAD
    errors = bq_upload.map(
        dataset_id=unmapped(constants.RDO_DATASET_ID.value),
        table_id=unmapped(table_id),
        filepath=treated_path,
        raw_filepath=raw_path,
        partitions=partitions,
        status=status,
    )
    set_redis = update_rdo_redis(
        download_files=download_files, table_id=table_id, errors=errors
    )

captura_sppo_rdo.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_sppo_rdo.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
# captura_sppo_rdo.schedule = every_day


# captura_sppo_rho = deepcopy(captura_sppo_rdo)
# captura_sppo_rho.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
# captura_sppo_rho.run_config = KubernetesRun(image=emd_constants.DOCKER_IMAGE.value)
