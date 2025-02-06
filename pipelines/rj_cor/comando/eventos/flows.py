# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flows for comando
"""

from datetime import timedelta

from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants

from pipelines.rj_cor.comando.eventos.constants import (
    constants as comando_constants,
)
from pipelines.rj_cor.comando.eventos.schedules import every_hour, every_month
from pipelines.rj_cor.comando.eventos.tasks import (
    download_data_ocorrencias,
    download_data_atividades,
    download_data_pops,
    get_date_interval,
    # get_redis_df,
    get_redis_max_date,
    save_data,
    save_no_partition,
    save_redis_max_date,
    treat_data_ocorrencias,
    treat_data_atividades,
)

from pipelines.rj_escritorio.rain_dashboard.constants import (
    constants as rain_dashboard_constants,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.dump_db.constants import constants as dump_db_constants
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.tasks import (
    get_current_flow_labels,
    create_table_and_upload_to_gcs,
)

with Flow(
    "COR: Comando - Ocorrências",
    code_owners=[
        "paty",
    ],
) as rj_cor_comando_eventos_flow:
    dump_mode = Parameter("dump_mode", default="append", required=False)

    # Materialization parameters
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_to_datario = Parameter(
        "materialize_to_datario", default=False, required=False
    )
    TRIGGER_RAIN_DASHBOARD_UPDATE = Parameter(
        "trigger_rain_dashboard_update", default=False, required=False
    )

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    dump_to_gcs = Parameter("dump_to_gcs", default=False, required=False)
    maximum_bytes_processed = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )

    # Get date interval from parameters
    first_date = Parameter("first_date", required=False, default=None)
    last_date = Parameter("last_date", required=False, default=None)

    # Redis interval mode
    redis_mode = Parameter("redis_mode", default="dev", required=False)

    dataset_id = comando_constants.DATASET_ID.value
    table_id = comando_constants.TABLE_ID_EVENTOS.value
    redis_name = comando_constants.REDIS_NAME.value

    first_date, last_date = get_date_interval(first_date, last_date)

    dfr = download_data_ocorrencias(first_date, last_date)

    redis_max_date = get_redis_max_date(
        dataset_id=dataset_id,
        table_id=table_id,
        name=redis_name,
        mode=redis_mode,
    )

    dfr_treated, redis_max_date = treat_data_ocorrencias(
        dfr,
        redis_max_date,
    )

    path = save_data(dfr_treated)
    task_upload = create_table_and_upload_to_gcs(
        data_path=path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=False,
        wait=path,
    )

    save_redis_max_date(
        dataset_id=dataset_id,
        table_id=table_id,
        name=redis_name,
        mode=redis_mode,
        redis_max_date=redis_max_date,
        wait=task_upload,
    )

    # save_redis_max_date.set_upstream(task_upload)

    # Warning: this task won't execute if we provide a date interval
    # on parameters. The reason this happens is for if we want to
    # perform backfills, it won't mess with the Redis interval.
    # with case(date_interval_text, None):
    #     set_redis_date_task = set_last_updated_on_redis(
    #         dataset_id=dataset_id,
    #         table_id=table_id_eventos,
    #         mode=redis_mode,
    #         current_time=current_time,
    #         problem_ids_atividade=problem_ids_atividade,
    #         # melhoria: adicionar forma de salvar os ids de atividades com problemas no backfill
    #     )
    #     set_redis_date_task.set_upstream(task_upload_eventos)
    #     set_redis_date_task.set_upstream(task_upload_atividade_eventos)

    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                # "table_id": table_id_eventos,
                "table_id": "ocorrencias",  # change to table_id
                "mode": materialization_mode,
                "materialize_to_datario": materialize_to_datario,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id}",
        )

        materialization_flow.set_upstream(task_upload)

        wait_for_materialization = wait_for_flow_run(
            materialization_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

        wait_for_materialization.max_retries = (
            dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
        )

        wait_for_materialization.retry_delay = timedelta(
            seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
        )

        with case(dump_to_gcs, True):
            # Trigger Dump to GCS flow run with project id as datario
            dump_to_gcs_flow = create_flow_run(
                flow_name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "project_id": "datario",
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "maximum_bytes_processed": maximum_bytes_processed,
                },
                labels=[
                    "datario",
                ],
                run_name=f"Dump to GCS {dataset_id}.{table_id}",
            )
            dump_to_gcs_flow.set_upstream(wait_for_materialization)

            wait_for_dump_to_gcs = wait_for_flow_run(
                dump_to_gcs_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )

    with case(TRIGGER_RAIN_DASHBOARD_UPDATE, True):
        # Trigger rain dashboard update flow run
        rain_radar_dashboard_update_flow = create_flow_run(
            flow_name=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters=comando_constants.RAIN_DASHBOARD_FLOW_SCHEDULE_PARAMETERS.value,  # noqa
            labels=[
                "rj-cor",
            ],
            run_name="Update radar rain dashboard data (triggered by cor_comando flow)",  # noqa
            task_args=dict(
                skip_on_upstream_skip=False,
            ),
        )
        rain_radar_dashboard_update_flow.set_upstream(task_upload)

        wait_for_rain_dashboard_update = wait_for_flow_run(
            flow_run_id=rain_radar_dashboard_update_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=False,
        )

        # Trigger rain dashboard update last 2h flow run
        rain_radar_dashboard_last_2h_update_flow = create_flow_run(
            flow_name=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters=comando_constants.RAIN_DASHBOARD_LAST_2H_FLOW_SCHEDULE_PARAMETERS.value,  # noqa
            labels=[
                "rj-cor",
            ],
            run_name="Update radar rain dashboard data (triggered by cor_comando flow for last 2h)",  # noqa
            task_args=dict(
                skip_on_upstream_skip=False,
            ),
        )
        rain_radar_dashboard_last_2h_update_flow.set_upstream(task_upload)

        wait_for_rain_dashboard_last_2h_update = wait_for_flow_run(
            flow_run_id=rain_radar_dashboard_last_2h_update_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=False,
        )

rj_cor_comando_eventos_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_cor_comando_eventos_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_COR_AGENT_LABEL.value,
    ],
)
rj_cor_comando_eventos_flow.schedule = every_hour


with Flow(
    "COR: Comando - Atividades do evento",
    code_owners=[
        "paty",
    ],
) as rj_cor_comando_atividades_evento_flow:
    dump_mode = Parameter("dump_mode", default="append", required=False)

    # Materialization parameters
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_to_datario = Parameter(
        "materialize_to_datario", default=False, required=False
    )
    TRIGGER_RAIN_DASHBOARD_UPDATE = Parameter(
        "trigger_rain_dashboard_update", default=False, required=False
    )

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    dump_to_gcs = Parameter("dump_to_gcs", default=False, required=False)
    maximum_bytes_processed = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )

    # Get date interval from parameters
    first_date = Parameter("first_date", required=False, default=None)
    last_date = Parameter("last_date", required=False, default=None)

    # Redis interval mode
    redis_mode = Parameter("redis_mode", default="dev", required=False)

    dataset_id = comando_constants.DATASET_ID.value
    redis_name = comando_constants.REDIS_NAME.value
    table_id = comando_constants.TABLE_ID_ATIVIDADES_EVENTOS.value

    first_date, last_date = get_date_interval(first_date, last_date)

    dfr = download_data_atividades(first_date, last_date)

    redis_max_date = get_redis_max_date(
        dataset_id=dataset_id,
        table_id=table_id,
        name=redis_name,
        mode=redis_mode,
    )

    dfr_treated, redis_max_date = treat_data_atividades(
        dfr,
        redis_max_date,
    )

    path = save_data(dfr_treated)

    task_upload = create_table_and_upload_to_gcs(
        data_path=path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=False,
        wait=path,
    )

    save_redis_max_date(
        dataset_id=dataset_id,
        table_id=table_id,
        name=redis_name,
        mode=redis_mode,
        redis_max_date=redis_max_date,
        wait=task_upload,
    )

    # Warning: this task won't execute if we provide a date interval
    # on parameters. The reason this happens is for if we want to
    # perform backfills, it won't mess with the Redis interval.
    # with case(date_interval_text, None):
    #     set_redis_date_task = set_last_updated_on_redis(
    #         dataset_id=dataset_id,
    #         table_id=table_id_eventos,
    #         mode=redis_mode,
    #         current_time=current_time,
    #         problem_ids_atividade=problem_ids_atividade,
    #         # melhoria: adicionar forma de salvar os ids de atividades com problemas no backfill
    #     )
    #     set_redis_date_task.set_upstream(task_upload_eventos)
    #     set_redis_date_task.set_upstream(task_upload_atividade_eventos)

    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()

        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "mode": materialization_mode,
                "materialize_to_datario": materialize_to_datario,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id}",
        )

        materialization_flow.set_upstream(task_upload)

        wait_for_materialization = wait_for_flow_run(
            materialization_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

        wait_for_materialization.max_retries = (
            dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
        )
        wait_for_materialization.retry_delay = timedelta(
            seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
        )

        with case(dump_to_gcs, True):
            # Trigger Dump to GCS flow run with project id as datario
            dump_to_gcs_flow = create_flow_run(
                flow_name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "project_id": "datario",
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "maximum_bytes_processed": maximum_bytes_processed,
                },
                labels=[
                    "datario",
                ],
                run_name=f"Dump to GCS {dataset_id}.{table_id}",
            )
            dump_to_gcs_flow.set_upstream(wait_for_materialization)

            wait_for_dump_to_gcs = wait_for_flow_run(
                dump_to_gcs_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )

rj_cor_comando_atividades_evento_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_cor_comando_atividades_evento_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_COR_AGENT_LABEL.value,
    ],
)
rj_cor_comando_atividades_evento_flow.schedule = every_hour


with Flow(
    "COR: Comando - POPs e Atividades dos POPs",
    code_owners=[
        "paty",
    ],
) as rj_cor_comando_pops_flow:
    # Dump mode
    dump_mode = Parameter("dump_mode", default="overwrite", required=False)

    # Materialization parameters
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_to_datario = Parameter(
        "materialize_to_datario", default=False, required=False
    )

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    dump_to_gcs = Parameter("dump_to_gcs", default=False, required=False)
    maximum_bytes_processed = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )

    dataset_id = Parameter(
        "dataset_id", default=comando_constants.DATASET_ID.value, required=False
    )
    table_id_pops = Parameter(
        "table_id_pops", default=comando_constants.TABLE_ID_POPS.value, required=False
    )

    pops = download_data_pops()
    path_pops = save_no_partition(dataframe=pops)

    task_upload_pops = create_table_and_upload_to_gcs(
        data_path=path_pops,
        dataset_id=dataset_id,
        table_id=table_id_pops,
        biglake_table=False,
        dump_mode=dump_mode,
    )

    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()

        materialization_pops_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id_pops,
                "mode": materialization_mode,
                "materialize_to_datario": materialize_to_datario,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id_pops}",
        )
        materialization_pops_flow.set_upstream(task_upload_pops)

        wait_for_pops_materialization = wait_for_flow_run(
            materialization_pops_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )
        wait_for_pops_materialization.max_retries = (
            dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
        )
        wait_for_pops_materialization.retry_delay = timedelta(
            seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
        )

        with case(dump_to_gcs, True):
            # Trigger Dump to GCS flow run with project id as datario
            dump_pops_to_gcs_flow = create_flow_run(
                flow_name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "project_id": "datario",
                    "dataset_id": dataset_id,
                    "table_id": table_id_pops,
                    "maximum_bytes_processed": maximum_bytes_processed,
                },
                labels=[
                    "datario",
                ],
                run_name=f"Dump to GCS {dataset_id}.{table_id_pops}",
            )
            dump_pops_to_gcs_flow.set_upstream(wait_for_pops_materialization)

            wait_for_dump_pops_to_gcs = wait_for_flow_run(
                dump_pops_to_gcs_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )

rj_cor_comando_pops_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_cor_comando_pops_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_COR_AGENT_LABEL.value,
    ],
)

rj_cor_comando_pops_flow.schedule = every_month
