# -*- coding: utf-8 -*-

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

from pipelines.rj_smtr.tasks import (
    create_date_hour_partition,
    create_local_partition_path,
    fetch_dataset_sha,
    get_current_timestamp,
    get_materialization_date_range,
    # get_local_dbt_client,
    get_raw,
    parse_timestamp_to_string,
    query_logs,
    save_raw_local,
    save_treated_local,
    set_last_run_timestamp,
    upload_logs_to_bq,
    bq_upload,
)
from pipelines.rj_smtr.br_rj_riodejaneiro_onibus_gps.tasks import (
    pre_treatment_br_rj_riodejaneiro_onibus_gps,
    create_api_url_onibus_gps,
    create_api_url_onibus_realocacao,
    pre_treatment_br_rj_riodejaneiro_onibus_realocacao,
)

from pipelines.rj_smtr.schedules import (
    every_hour_minute_six,
    every_minute,
    every_10_minutes,
)
from pipelines.utils.execute_dbt_model.tasks import run_dbt_model
from pipelines.utils.utils import skip_if_running_handler

# Flows #

with Flow(
    "SMTR: GPS SPPO Realocação - Captura",
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as realocacao_sppo:
    # SETUP #

    # Get default parameters #
    raw_dataset_id = Parameter(
        "raw_dataset_id", default=constants.GPS_SPPO_RAW_DATASET_ID.value
    )
    raw_table_id = Parameter(
        "raw_table_id", default=constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value
    )
    dataset_id = Parameter("dataset_id", default=constants.GPS_SPPO_DATASET_ID.value)
    table_id = Parameter(
        "table_id", default=constants.GPS_SPPO_REALOCACAO_TREATED_TABLE_ID.value
    )
    rebuild = Parameter("rebuild", False)

    # SETUP
    timestamp = get_current_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=realocacao_sppo.name + ": ", now_time=timestamp
    )

    partitions = create_date_hour_partition(timestamp)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        table_id=constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value,
        filename=filename,
        partitions=partitions,
    )

    url = create_api_url_onibus_realocacao(timestamp=timestamp)

    # EXTRACT #
    raw_status = get_raw(url)

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

    # CLEAN #
    treated_status = pre_treatment_br_rj_riodejaneiro_onibus_realocacao(
        status=raw_status, timestamp=timestamp
    )

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD #
    error = bq_upload(
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        table_id=constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )

    upload_logs_to_bq(
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        parent_table_id=constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value,
        error=error,
        timestamp=timestamp,
    )

realocacao_sppo.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
realocacao_sppo.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
realocacao_sppo.schedule = every_10_minutes


with Flow(
    "SMTR: GPS SPPO - Materialização (subflow)",
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as materialize_sppo:
    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix=materialize_sppo.name + ": ", now_time=get_now_time()
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
        table_run_datetime_column_name="timestamp_gps",
        mode=MODE,
        delay_hours=constants.GPS_SPPO_MATERIALIZE_DELAY_HOURS.value,
    )
    dataset_sha = fetch_dataset_sha(
        dataset_id=dataset_id,
    )

    # Run materialization #
    with case(rebuild, True):
        RUN = run_dbt_model(
            dbt_client=dbt_client,
            dataset_id=dataset_id,
            table_id=table_id,
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
            dataset_id=dataset_id,
            table_id=table_id,
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
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
materialize_sppo.state_handlers.append(skip_if_running_handler)


with Flow(
    "SMTR: GPS SPPO - Captura",
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as captura_sppo_v2:
    version = Parameter("version", default=2)

    # SETUP #
    timestamp = get_current_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=captura_sppo_v2.name + ": ", now_time=timestamp
    )

    partitions = create_date_hour_partition(timestamp)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        table_id=constants.GPS_SPPO_RAW_TABLE_ID.value,
        filename=filename,
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

    upload_logs_to_bq(
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        parent_table_id=constants.GPS_SPPO_RAW_TABLE_ID.value,
        error=error,
        timestamp=timestamp,
    )

captura_sppo_v2.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_sppo_v2.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
captura_sppo_v2.schedule = every_minute

with Flow(
    "SMTR: GPS SPPO Realocação - Recaptura (subflow)",
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as recaptura_realocacao_sppo:
    version = Parameter("version", default=2)
    datetime_filter = Parameter("datetime_filter", default=None)
    materialize = Parameter("materialize", default=True)

    # SETUP #
    LABELS = get_current_flow_labels()

    # Consulta de logs para verificar erros
    errors, timestamps, previous_errors = query_logs(
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        table_id=constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value,
        datetime_filter=datetime_filter,
    )

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=recaptura_realocacao_sppo.name + ": ",
        now_time=get_now_time(),
        wait=timestamps,
    )

    # Em caso de erros, executa a recaptura
    with case(errors, True):
        # SETUP #
        partitions = create_date_hour_partition.map(timestamps)
        filename = parse_timestamp_to_string.map(timestamps)

        filepath = create_local_partition_path.map(
            dataset_id=unmapped(constants.GPS_SPPO_RAW_DATASET_ID.value),
            table_id=unmapped(constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value),
            filename=filename,
            partitions=partitions,
        )

        url = create_api_url_onibus_realocacao.map(
            version=unmapped(version), timestamp=timestamps
        )

        # EXTRACT #
        raw_status = get_raw.map(url)

        raw_filepath = save_raw_local.map(status=raw_status, file_path=filepath)

        # CLEAN #
        treated_status = pre_treatment_br_rj_riodejaneiro_onibus_realocacao.map(
            status=raw_status, timestamp=timestamps, version=unmapped(version)
        )

        treated_filepath = save_treated_local.map(
            status=treated_status, file_path=filepath
        )

        # LOAD #
        error = bq_upload.map(
            dataset_id=unmapped(constants.GPS_SPPO_RAW_DATASET_ID.value),
            table_id=unmapped(constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value),
            filepath=treated_filepath,
            raw_filepath=raw_filepath,
            partitions=partitions,
            status=treated_status,
        )

        upload_logs_to_bq.map(
            dataset_id=unmapped(constants.GPS_SPPO_RAW_DATASET_ID.value),
            parent_table_id=unmapped(constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value),
            error=error,
            previous_error=previous_errors,
            timestamp=timestamps,
            recapture=unmapped(True),
        )

recaptura_realocacao_sppo.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
recaptura_realocacao_sppo.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

with Flow(
    "SMTR: GPS SPPO - Tratamento", code_owners=["caio", "fernanda", "boris", "rodrigo"]
) as recaptura:
    version = Parameter("version", default=2)
    datetime_filter = Parameter("datetime_filter", default=None)
    materialize = Parameter("materialize", default=True)
    # SETUP #
    LABELS = get_current_flow_labels()

    # roda o subflow de reacptura da realocação
    run_recaptura_realocacao_sppo = create_flow_run(
        flow_name=recaptura_realocacao_sppo.name,
        project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
        labels=LABELS,
        run_name=recaptura_realocacao_sppo.name,
    )

    wait_recaptura_realocacao_sppo = wait_for_flow_run(
        run_recaptura_realocacao_sppo,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

    errors, timestamps, previous_errors = query_logs(
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        table_id=constants.GPS_SPPO_RAW_TABLE_ID.value,
        datetime_filter=datetime_filter,
    )

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=recaptura.name + ": ", now_time=get_now_time(), wait=timestamps
    )
    with case(errors, False):
        with case(materialize, True):
            materialize_no_error = create_flow_run(
                flow_name=materialize_sppo.name,
                project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
                labels=LABELS,
                run_name=materialize_sppo.name,
            )
            wait_materialize_no_error = wait_for_flow_run(
                materialize_no_error,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )
    with case(errors, True):
        # SETUP #
        partitions = create_date_hour_partition.map(timestamps)
        filename = parse_timestamp_to_string.map(timestamps)

        filepath = create_local_partition_path.map(
            dataset_id=unmapped(constants.GPS_SPPO_RAW_DATASET_ID.value),
            table_id=unmapped(constants.GPS_SPPO_RAW_TABLE_ID.value),
            filename=filename,
            partitions=partitions,
        )

        url = create_api_url_onibus_gps.map(
            version=unmapped(version), timestamp=timestamps
        )

        # EXTRACT #
        raw_status = get_raw.map(url)

        raw_filepath = save_raw_local.map(status=raw_status, file_path=filepath)

        # # CLEAN #
        trated_status = pre_treatment_br_rj_riodejaneiro_onibus_gps.map(
            status=raw_status,
            timestamp=timestamps,
            version=unmapped(version),
        )

        treated_filepath = save_treated_local.map(
            status=trated_status, file_path=filepath
        )

        # # LOAD #
        error = bq_upload.map(
            dataset_id=unmapped(constants.GPS_SPPO_RAW_DATASET_ID.value),
            table_id=unmapped(constants.GPS_SPPO_RAW_TABLE_ID.value),
            filepath=treated_filepath,
            raw_filepath=raw_filepath,
            partitions=partitions,
            status=trated_status,
        )

        UPLOAD_LOGS = upload_logs_to_bq.map(
            dataset_id=unmapped(constants.GPS_SPPO_RAW_DATASET_ID.value),
            parent_table_id=unmapped(constants.GPS_SPPO_RAW_TABLE_ID.value),
            error=error,
            previous_error=previous_errors,
            timestamp=timestamps,
            recapture=unmapped(True),
        )
        with case(materialize, True):
            run_materialize = create_flow_run(
                flow_name=materialize_sppo.name,
                project_name=emd_constants.PREFECT_DEFAULT_PROJECT.value,
                labels=LABELS,
                run_name=materialize_sppo.name,
            )
            wait_materialize = wait_for_flow_run(
                run_materialize,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )
    recaptura.set_dependencies(
        task=run_materialize,
        upstream_tasks=[UPLOAD_LOGS],
    )

recaptura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
recaptura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

recaptura.state_handlers.append(skip_if_running_handler)

recaptura.schedule = every_hour_minute_six
