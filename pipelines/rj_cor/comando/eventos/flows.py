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
    download_eventos,
    get_atividades_pops,
    get_date_interval,
    get_pops,
    salvar_dados,
    save_no_partition,
    set_last_updated_on_redis,
)
from pipelines.rj_cor.tasks import (
    get_on_redis,
    save_on_redis,
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
    "COR: Comando - Eventos e Atividades do Evento",
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

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    dump_to_gcs = Parameter("dump_to_gcs", default=False, required=False)
    maximum_bytes_processed = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )

    # Get date interval from parameters
    date_interval_text = Parameter("date_interval", required=False, default=None)

    # Redis interval mode
    redis_mode = Parameter("redis_mode", default="dev", required=False)

    dataset_id = comando_constants.DATASET_ID.value
    table_id_eventos = comando_constants.TABLE_ID_EVENTOS.value
    table_id_atividades_eventos = comando_constants.TABLE_ID_ATIVIDADES_EVENTOS.value

    date_interval, current_time = get_date_interval(
        date_interval_text=date_interval_text,
        dataset_id=dataset_id,
        table_id=table_id_eventos,
        mode=redis_mode,
    )

    eventos, atividade_eventos, problem_ids_atividade = download_eventos(
        date_interval=date_interval, wait=current_time
    )
    eventos_path = salvar_dados(
        dfr=eventos, current_time=date_interval["fim"], name="eventos"
    )
    atividade_eventos_path = salvar_dados(
        dfr=atividade_eventos,
        current_time=date_interval["fim"],
        name="atividade_eventos",
    )

    task_upload_eventos = create_table_and_upload_to_gcs(
        data_path=eventos_path,
        dataset_id=dataset_id,
        table_id=table_id_eventos,
        dump_mode=dump_mode,
        wait=eventos_path,
    )

    task_upload_atividade_eventos = create_table_and_upload_to_gcs(
        data_path=atividade_eventos_path,
        dataset_id=dataset_id,
        table_id=table_id_atividades_eventos,
        dump_mode=dump_mode,
        wait=atividade_eventos_path,
    )

    # Warning: this task won't execute if we provide a date interval
    # on parameters. The reason this happens is for if we want to
    # perform backfills, it won't mess with the Redis interval.
    with case(date_interval_text, None):
        set_redis_date_task = set_last_updated_on_redis(
            dataset_id=dataset_id,
            table_id=table_id_eventos,
            mode=redis_mode,
            current_time=current_time,
            problem_ids_atividade=problem_ids_atividade,
            # melhoria: adicionar forma de salvar os ids de atividades com problemas no backfill
        )
        set_redis_date_task.set_upstream(task_upload_eventos)
        set_redis_date_task.set_upstream(task_upload_atividade_eventos)

    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        eventos_materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id_eventos,
                "mode": materialization_mode,
                "materialize_to_datario": materialize_to_datario,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id_eventos}",
        )
        atividade_eventos_materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id_atividades_eventos,
                "mode": materialization_mode,
                "materialize_to_datario": materialize_to_datario,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id_atividades_eventos}",
        )

        wait_for_eventos_materialization = wait_for_flow_run(
            eventos_materialization_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

        wait_for_atividade_eventos_materialization = wait_for_flow_run(
            atividade_eventos_materialization_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

        wait_for_eventos_materialization.max_retries = (
            dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
        )

        wait_for_atividade_eventos_materialization.max_retries = (
            dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
        )

        wait_for_eventos_materialization.retry_delay = timedelta(
            seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
        )

        wait_for_atividade_eventos_materialization.retry_delay = timedelta(
            seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
        )

        with case(dump_to_gcs, True):
            # Trigger Dump to GCS flow run with project id as datario
            dump_eventos_to_gcs_flow = create_flow_run(
                flow_name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "project_id": "datario",
                    "dataset_id": dataset_id,
                    "table_id": table_id_eventos,
                    "maximum_bytes_processed": maximum_bytes_processed,
                },
                labels=[
                    "datario",
                ],
                run_name=f"Dump to GCS {dataset_id}.{table_id_eventos}",
            )
            dump_eventos_to_gcs_flow.set_upstream(wait_for_eventos_materialization)

            dump_atividade_eventos_to_gcs_flow = create_flow_run(
                flow_name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "project_id": "datario",
                    "dataset_id": dataset_id,
                    "table_id": table_id_atividades_eventos,
                    "maximum_bytes_processed": maximum_bytes_processed,
                },
                labels=[
                    "datario",
                ],
                run_name=f"Dump to GCS {dataset_id}.{table_id_atividades_eventos}",
            )
            dump_atividade_eventos_to_gcs_flow.set_upstream(
                wait_for_atividade_eventos_materialization
            )

            wait_for_dump_to_gcs = wait_for_flow_run(
                dump_eventos_to_gcs_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )

            wait_for_dump_atividade_eventos_to_gcs = wait_for_flow_run(
                dump_atividade_eventos_to_gcs_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
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
    table_id_atividades_pops = Parameter(
        "table_id_atividades_pops",
        default=comando_constants.TABLE_ID_ATIVIDADES_POPS.value,
        required=False,
    )

    pops = get_pops()
    redis_pops = get_on_redis(dataset_id, table_id_atividades_pops, mode="dev")
    atividades_pops, update_pops_redis = get_atividades_pops(
        pops=pops, redis_pops=redis_pops
    )

    path_pops = save_no_partition(dataframe=pops)
    path_atividades_pops = save_no_partition(dataframe=atividades_pops, append=True)

    task_upload_pops = create_table_and_upload_to_gcs(
        data_path=path_pops,
        dataset_id=dataset_id,
        table_id=table_id_pops,
        dump_mode=dump_mode,
    )

    task_upload_atividades_pops = create_table_and_upload_to_gcs(
        data_path=path_atividades_pops,
        dataset_id=dataset_id,
        table_id=table_id_atividades_pops,
        dump_mode=dump_mode,
    )

    save_on_redis(
        dataset_id,
        table_id_atividades_pops,
        "dev",
        update_pops_redis,
        wait=task_upload_atividades_pops,
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

        materialization_atividades_pops_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id_atividades_pops,
                "mode": materialization_mode,
                "materialize_to_datario": materialize_to_datario,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id_atividades_pops}",
        )
        materialization_atividades_pops_flow.set_upstream(task_upload_atividades_pops)

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

        wait_for_atividades_pops_materialization = wait_for_flow_run(
            materialization_atividades_pops_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )
        wait_for_atividades_pops_materialization.max_retries = (
            dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
        )
        wait_for_atividades_pops_materialization.retry_delay = timedelta(
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

            dump_atividades_pops_to_gcs_flow = create_flow_run(
                flow_name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "project_id": "datario",
                    "dataset_id": dataset_id,
                    "table_id": table_id_atividades_pops,
                    "maximum_bytes_processed": maximum_bytes_processed,
                },
                labels=[
                    "datario",
                ],
                run_name=f"Dump to GCS {dataset_id}.{table_id_atividades_pops}",
            )
            dump_atividades_pops_to_gcs_flow.set_upstream(
                wait_for_atividades_pops_materialization
            )

            wait_for_dump_pops_to_gcs = wait_for_flow_run(
                dump_pops_to_gcs_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )

            wait_for_dump_atividades_pops_to_gcs = wait_for_flow_run(
                dump_atividades_pops_to_gcs_flow,
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
