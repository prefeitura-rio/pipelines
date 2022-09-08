# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_onibus_gps
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.tasks.control_flow import merge
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
    every_hour_minute_six,
)
from pipelines.rj_smtr.tasks import (
    create_date_hour_partition,
    create_local_partition_path,
    fetch_dataset_sha,
    get_current_timestamp,
    get_materialization_date_range,
    # get_local_dbt_client,
    get_raw,
    get_bool,
    # parse_timestamp_to_string,
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
    create_api_url_onibus_gps,
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

materialize_sppo.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
materialize_sppo.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)


with Flow(
    "SMTR: GPS SPPO - Captura",
    code_owners=["caio", "fernanda"],
) as captura_sppo_v2:

    version = Parameter("version", default=2)

    # RECAPTURE PARAMETERS
    datetime_filter = Parameter("datetime_filter", default=None)
    recapture = Parameter("recapture", default=False)
    previous_error = Parameter("previous_error", default=None)

    # SETUP #
    with case(get_bool(datetime_filter), False):
        timestamp_now = get_current_timestamp()
    with case(get_bool(datetime_filter), True):
        timestamp_default = datetime_filter

    timestamp = merge(timestamp_default, timestamp_now)

    rename_flow_run = rename_current_flow_run_now_time(
        prefix="GPS SPPO: ", now_time=timestamp
    )

    partitions = create_date_hour_partition(timestamp)

    # filename = parse_timestamp_to_string(timestamp)  # porquê do pattern escolhido?

    filepath = create_local_partition_path(
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        table_id=constants.GPS_SPPO_RAW_TABLE_ID.value,
        filename=timestamp,
        partitions=partitions,
    )

    url = create_api_url_onibus_gps(version=version, timestamp=timestamp)

    # EXTRACT #
    raw_status = get_raw(url)

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

    # CLEAN #
    treated_status = pre_treatment_br_rj_riodejaneiro_onibus_gps(
        status=raw_status, timestamp=timestamp, version=version
    )

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD #
    error = bq_upload(
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        table_id=constants.GPS_SPPO_RAW_TABLE_ID.value,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )
    with case(get_bool(error), False):
        upload_logs_to_bq(
            dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
            parent_table_id=constants.GPS_SPPO_RAW_TABLE_ID.value,
            error=previous_error,
            timestamp=timestamp,
            recapture=recapture,
        )
    with case(get_bool(error), True):
        upload_logs_to_bq(
            dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
            parent_table_id=constants.GPS_SPPO_RAW_TABLE_ID.value,
            error=error,
            timestamp=timestamp,
            recapture=recapture,
        )


captura_sppo_v2.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_sppo_v2.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
    cpu_limit="50m",
    cpu_request="10m",
    memory_limit="256Mi",
    memory_request="64Mi",
)
captura_sppo_v2.schedule = every_minute


with Flow("SMTR - GPS SPPO Recapturas", code_owners=["caio", "fernanda"]) as recaptura:

    version = Parameter("version", default=2)
    datetime_filter = Parameter("datetime_filter", default=None)
    # SETUP #
    LABELS = get_current_flow_labels()
    errors, recapture_parameters = query_logs(
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        table_id=constants.GPS_SPPO_RAW_TABLE_ID.value,
        datetime_filter=datetime_filter,
    )

    rename_flow_run = rename_current_flow_run_now_time(
        prefix="GPS SPPO Recapturas: ",
        now_time=get_now_time(),
        wait=recapture_parameters,
    )

    with case(errors, False):
        materialize_no_errors = create_flow_run(
            flow_name=materialize_sppo.name,
            project_name="staging",
            labels=LABELS,
            run_name=materialize_sppo.name,
        )
        wait_materialize = wait_for_flow_run(
            materialize_no_errors,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )
    with case(errors, True):
        recaptura_runs = create_flow_run.map(
            flow_name=unmapped(captura_sppo_v2.name),
            project_name=unmapped("staging"),
            labels=unmapped(LABELS),
            run_name=unmapped(captura_sppo_v2.name),
            parameters=recapture_parameters,
        )
        wait_for_recapturas = wait_for_flow_run.map(
            recaptura_runs,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )
        materialize = create_flow_run(
            flow_name=materialize_sppo.name,
            project_name="staging",
            labels=LABELS,
            run_name=materialize_sppo.name,
        )
        wait_for_materialize = wait_for_flow_run(
            materialize, stream_states=True, stream_logs=True, raise_final_state=True
        )
    materialize.set_upstream(wait_for_recapturas)
recaptura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
recaptura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
    memory_limit="256Mi",
    memory_request="64Mi",
    cpu_limit="50m",
)
recaptura.schedule = every_hour_minute_six
