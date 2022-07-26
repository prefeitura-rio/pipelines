# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_onibus_gps
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped


# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
    get_now_time,
    get_current_flow_mode,
    get_current_flow_labels,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client

# SMTR Imports #

from pipelines.rj_smtr.constants import constants

from pipelines.rj_smtr.schedules import (
    every_minute,
    every_hour,
)
from pipelines.rj_smtr.tasks import (
    create_current_date_hour_partition,
    create_local_partition_path,
    delay_now_time,
    fetch_dataset_sha,
    get_materialization_date_range,
    # get_local_dbt_client,
    get_raw,
    query_logs,
    run_dbt_model,
    save_raw_local,
    save_treated_local,
    set_last_run_timestamp,
    upload_logs_to_bq,
    bq_upload,
)
from pipelines.rj_smtr.br_rj_riodejaneiro_onibus_gps.tasks import (
    pre_treatment_br_rj_riodejaneiro_onibus_gps,
)

# Flows #

with Flow(
    "SMTR: GPS SPPO - Materialização",
    code_owners=["caio", "fernanda"],
) as materialize_sppo:
    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix="SMTR: GPS SPPO - Materialização - ", now_time=get_now_time()
    )

    # Get default parameters #
    raw_dataset_id = Parameter(
        "raw_dataset_id", default=constants.GPS_SPPO_RAW_DATASET_ID.value
    )
    raw_table_id = Parameter(
        "raw_table_id", default=constants.GPS_SPPO_RAW_TABLE_ID.value
    )
    dataset_id = Parameter("dataset_id", default=constants.GPS_SPPO_DATASET_ID.value)
    table_id = Parameter("table_id", default=constants.GPS_SPPO_TREATED_TABLE_ID.value)
    rebuild = Parameter("rebuild", False)

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    # Set dbt client #
    dbt_client = get_k8s_dbt_client(mode=MODE, wait=rename_flow_run)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    # Set specific run parameters #
    date_range = get_materialization_date_range(
        dataset_id=dataset_id,
        table_id=table_id,
        raw_dataset_id=raw_dataset_id,
        raw_table_id=raw_table_id,
        table_date_column_name="data",
        mode=MODE,
    )
    dataset_sha = fetch_dataset_sha(
        dataset_id=dataset_id,
    )

    # Run materialization #
    with case(rebuild, True):
        RUN = run_dbt_model(
            dbt_client=dbt_client,
            model=table_id,
            upstream=True,
            exclude="+data_versao_efetiva",
            _vars=[date_range, dataset_sha],
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
            model=table_id,
            exclude="+data_versao_efetiva",
            _vars=[date_range, dataset_sha],
            upstream=True,
        )
        set_last_run_timestamp(
            dataset_id=dataset_id,
            table_id=table_id,
            timestamp=date_range["date_range_end"],
            wait=RUN,
            mode=MODE,
        )


with Flow(
    "SMTR: GPS SPPO - Captura",
    code_owners=["caio", "fernanda"],
) as captura_sppo_v2:

    # Get default parameters #
    dataset_id = Parameter(
        "dataset_id", default=constants.GPS_SPPO_RAW_DATASET_ID.value
    )
    table_id = Parameter("table_id", default=constants.GPS_SPPO_RAW_TABLE_ID.value)
    url = Parameter("url", default=constants.GPS_SPPO_API_BASE_URL_V2.value)
    secret_path = Parameter(
        "secret_path", default=constants.GPS_SPPO_API_SECRET_PATH_V2.value
    )
    version = Parameter("version", default=2)

    # Run tasks #
    file_dict = create_current_date_hour_partition()

    filepath = create_local_partition_path(
        dataset_id=dataset_id,
        table_id=table_id,
        filename=file_dict["filename"],
        partitions=file_dict["partitions"],
    )

    status_dict = get_raw(url=url, source=secret_path)

    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix="GPS SPPO: ", now_time=delay_now_time(status_dict["timestamp"])
    )

    raw_filepath = save_raw_local(data=status_dict["data"], file_path=filepath)

    treated_status = pre_treatment_br_rj_riodejaneiro_onibus_gps(
        status_dict=status_dict, version=version
    )

    UPLOAD_LOGS = upload_logs_to_bq(
        dataset_id=dataset_id,
        parent_table_id=table_id,
        timestamp=status_dict["timestamp"],
        error=status_dict["error"],
    )

    treated_filepath = save_treated_local(
        dataframe=treated_status["df"], file_path=filepath
    )

    UPLOAD_CSV = bq_upload(
        dataset_id=dataset_id,
        table_id=table_id,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=file_dict["partitions"],
    )
    captura_sppo_v2.set_dependencies(task=status_dict, upstream_tasks=[filepath])

with Flow("SMTR - GPS SPPO Recapturas", code_owners=["caio", "fernanda"]) as recaptura:
    # Get default parameters
    dataset_id = Parameter(
        "dataset_id", default=constants.GPS_SPPO_RAW_DATASET_ID.value
    )
    table_id = Parameter("table_id", default=constants.GPS_SPPO_RAW_TABLE_ID.value)
    url = Parameter("url", default=constants.GPS_SPPO_API_BASE_URL_V2.value)
    secret_path = Parameter(
        "secret_path", default=constants.GPS_SPPO_API_SECRET_PATH_V2.value
    )
    version = Parameter("version", default=2)

    # Run tasks
    errors, timestamps = query_logs(dataset_id=dataset_id, table_id=table_id)
    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix="GPS SPPO: ", now_time=get_now_time(), wait=timestamps
    )
    with case(errors, False):
        materialize = create_flow_run(
            flow_name=materialize_sppo.name,
            project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
            labels=LABELS,
            run_name=materialize_sppo.name,
        )
        wait_materialize = wait_for_flow_run(
            materialize,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )
    with case(errors, True):
        file_dict = create_current_date_hour_partition.map(capture_time=timestamps)
        filepath = create_local_partition_path.map(
            dataset_id=unmapped(dataset_id),
            table_id=unmapped(table_id),
            file_dict=file_dict,
        )
        status_dict = get_raw.map(
            url=unmapped(url), source=unmapped(secret_path), timestamp=timestamps
        )
        raw_filepath = save_raw_local.map(status_dict=status_dict, file_path=filepath)
        treated_status = pre_treatment_br_rj_riodejaneiro_onibus_gps.map(
            status_dict=status_dict, version=unmapped(version), recapture=unmapped(True)
        )

        UPLOAD_LOGS = upload_logs_to_bq.map(
            dataset_id=unmapped(dataset_id),
            parent_table_id=unmapped(table_id),
            status_dict=status_dict,
        )

        treated_filepath = save_treated_local.map(
            treated_status=treated_status, file_path=filepath
        )

        UPLOAD_CSV = bq_upload.map(
            dataset_id=unmapped(dataset_id),
            table_id=unmapped(table_id),
            filepath=treated_filepath,
            raw_filepath=raw_filepath,
            file_dict=file_dict,
        )
        materialize = create_flow_run(
            flow_name=materialize_sppo.name,
            project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
            labels=LABELS,
            run_name=materialize_sppo.name,
        )
        wait_materialize = wait_for_flow_run(
            materialize,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )
    recaptura.set_dependencies(task=status_dict, upstream_tasks=[filepath])
    recaptura.set_dependencies(task=materialize, upstream_tasks=[UPLOAD_CSV])

recaptura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
recaptura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
recaptura.schedule = every_hour

materialize_sppo.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
materialize_sppo.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

captura_sppo_v2.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_sppo_v2.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
captura_sppo_v2.schedule = every_minute
