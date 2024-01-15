# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flows for precipitacao_alertario.
"""
from datetime import timedelta

from prefect import case, Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.custom import wait_for_flow_run_with_timeout
from pipelines.rj_cor.meteorologia.precipitacao_alertario.constants import (
    constants as alertario_constants,
)
from pipelines.rj_cor.meteorologia.precipitacao_alertario.tasks import (
    check_to_run_dbt,
    dump_from_ssh,
    tratar_dados,
    treat_dumped_data,
    salvar_dados,
    save_last_dbt_update,
)
from pipelines.rj_cor.meteorologia.precipitacao_alertario.schedules import (
    minute_schedule,
    day_schedule,
)
from pipelines.rj_escritorio.rain_dashboard.constants import (
    constants as rain_dashboard_constants,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.dump_db.constants import constants as dump_db_constants
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
)

wait_for_flow_run_with_2min_timeout = wait_for_flow_run_with_timeout(
    timeout=timedelta(minutes=2)
)

with Flow(
    name="COR: Meteorologia - Precipitacao ALERTARIO",
    code_owners=[
        "paty",
    ],
    # skip_if_running=True,
) as cor_meteorologia_precipitacao_alertario:
    DATASET_ID = "clima_pluviometro"
    TABLE_ID = "taxa_precipitacao_alertario"
    DUMP_MODE = "append"

    # Materialization parameters
    MATERIALIZE_AFTER_DUMP = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    MATERIALIZE_TO_DATARIO = Parameter(
        "materialize_to_datario", default=False, required=False
    )
    MATERIALIZATION_MODE = Parameter("mode", default="dev", required=False)
    TRIGGER_RAIN_DASHBOARD_UPDATE = Parameter(
        "trigger_rain_dashboard_update", default=False, required=False
    )

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    DUMP_TO_GCS = Parameter("dump_to_gcs", default=False, required=False)

    MAXIMUM_BYTES_PROCESSED = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )

    dados, empty_data = tratar_dados(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        mode=MATERIALIZATION_MODE,
    )

    with case(empty_data, False):
        path = salvar_dados(dados=dados)
        # Create table in BigQuery
        UPLOAD_TABLE = create_table_and_upload_to_gcs(
            data_path=path,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            dump_mode=DUMP_MODE,
            wait=path,
        )

    run_dbt = check_to_run_dbt(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        mode=MATERIALIZATION_MODE,
    )
    run_dbt.set_upstream(UPLOAD_TABLE)

    with case(run_dbt, True):
        # Trigger DBT flow run
        with case(MATERIALIZE_AFTER_DUMP, True):
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": DATASET_ID,
                    "table_id": TABLE_ID,
                    "mode": MATERIALIZATION_MODE,
                    "materialize_to_datario": MATERIALIZE_TO_DATARIO,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {DATASET_ID}.{TABLE_ID}",
            )

            current_flow_labels.set_upstream(run_dbt)
            materialization_flow.set_upstream(current_flow_labels)

            wait_for_materialization = wait_for_flow_run_with_2min_timeout(
                flow_run_id=materialization_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )

            last_dbt_update = save_last_dbt_update(
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                mode=MATERIALIZATION_MODE,
                wait=wait_for_materialization,
            )

            wait_for_materialization.max_retries = (
                dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
            )
            wait_for_materialization.retry_delay = timedelta(
                seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
            )

            with case(TRIGGER_RAIN_DASHBOARD_UPDATE, True):
                # Trigger rain dashboard update flow run
                rain_dashboard_update_flow = create_flow_run(
                    flow_name=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_NAME.value,
                    project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                    parameters=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_SCHEDULE_PARAMETERS.value,  # noqa
                    labels=[
                        "rj-escritorio-dev",
                    ],
                    run_name="Update rain dashboard data (triggered by precipitacao_alertario flow)",  # noqa
                )
                rain_dashboard_update_flow.set_upstream(wait_for_materialization)

                wait_for_rain_dashboard_update = wait_for_flow_run(
                    flow_run_id=rain_dashboard_update_flow,
                    stream_states=True,
                    stream_logs=True,
                    raise_final_state=False,
                )

                # Trigger rain dashboard update last 2h flow run
                rain_dashboard_last_2h_update_flow = create_flow_run(
                    flow_name=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_NAME.value,
                    project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                    parameters=alertario_constants.RAIN_DASHBOARD_LAST_2H_FLOW_SCHEDULE_PARAMETERS.value,  # noqa
                    labels=[
                        "rj-escritorio-dev",
                    ],
                    run_name="Update rain dashboard data (triggered by precipitacao_alertario last 2h flow)",  # noqa
                )
                rain_dashboard_last_2h_update_flow.set_upstream(
                    wait_for_materialization
                )

                wait_for_rain_dashboard_last_2h_update = wait_for_flow_run(
                    flow_run_id=rain_dashboard_last_2h_update_flow,
                    stream_states=True,
                    stream_logs=True,
                    raise_final_state=False,
                )

            with case(DUMP_TO_GCS, True):
                # Trigger Dump to GCS flow run with project id as datario
                dump_to_gcs_flow = create_flow_run(
                    flow_name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
                    project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                    parameters={
                        "project_id": "datario",
                        "dataset_id": DATASET_ID,
                        "table_id": TABLE_ID,
                        "maximum_bytes_processed": MAXIMUM_BYTES_PROCESSED,
                    },
                    labels=[
                        "datario",
                    ],
                    run_name=f"Dump to GCS {DATASET_ID}.{TABLE_ID}",
                )
                dump_to_gcs_flow.set_upstream(wait_for_materialization)

                wait_for_dump_to_gcs = wait_for_flow_run_with_2min_timeout(
                    flow_run_id=dump_to_gcs_flow,
                    stream_states=True,
                    stream_logs=True,
                    raise_final_state=True,
                )

# para rodar na cloud
cor_meteorologia_precipitacao_alertario.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_precipitacao_alertario.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_precipitacao_alertario.executor = LocalDaskExecutor(num_workers=10)
cor_meteorologia_precipitacao_alertario.schedule = minute_schedule

with Flow(
    name="COR: Meteorologia - Precipitacao ALERTARIO Backfill",
    code_owners=[
        "paty",
    ],
    # skip_if_running=True,
) as cor_meteorologia_precipitacao_alertario_backfill:
    mode = Parameter("mode", default="prod", required=False)
    start_date = Parameter("start_date", default=None, required=False)
    end_date = Parameter("end_date", default=None, required=False)

    filepath = dump_from_ssh()
    treat_dumped_data(filepath)
    # dfr = treat_dumped_data(filepath, start_date, end_date)

# para rodar na cloud
cor_meteorologia_precipitacao_alertario_backfill.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
cor_meteorologia_precipitacao_alertario_backfill.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_precipitacao_alertario_backfill.schedule = day_schedule
