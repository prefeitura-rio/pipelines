# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Vitai heathrecord dumping flows
"""
from datetime import timedelta

from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import get_current_flow_labels
from pipelines.utils.dump_db.constants import constants as dump_db_constants
from pipelines.constants import constants
from pipelines.rj_sms.dump_api_vitai.contants import constants as vitai_constants
from pipelines.rj_sms.utils import (
    create_folders,
    from_json_to_csv,
    download_from_api,
    add_load_date_column,
    create_partitions,
    upload_to_datalake,
)
from pipelines.rj_sms.dump_api_vitai.tasks import (
    build_movimentos_date,
    build_movimentos_url,
)
from pipelines.rj_sms.dump_api_vitai.scheduler import every_day_at_six_am


with Flow(
    name="SMS: Dump Vitai - Captura Posição de Estoque", code_owners=["thiago"]
) as dump_vitai_posicao:
    # Parameters
    # Parameters for Vault
    vault_path = vitai_constants.VAULT_PATH.value
    vault_key = vitai_constants.VAULT_KEY.value
    # Paramenters for GCP
    dataset_id = vitai_constants.DATASET_ID.value
    table_id = vitai_constants.TABLE_POSICAO_ID.value
    # Parameters for materialization
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    materialize_to_datario = Parameter(
        "materialize_to_datario", default=False, required=False
    )
    materialization_mode = Parameter("mode", default="dev", required=False)

    # Start run
    create_folders_task = create_folders()

    download_task = download_from_api(
        url="https://apidw.vitai.care/api/dw/v1/produtos/saldoAtual",
        params=None,
        file_folder="./data/raw",
        file_name=table_id,
        vault_path=vault_path,
        vault_key=vault_key,
        add_load_date_to_filename=True,
    )
    download_task.set_upstream(create_folders_task)

    conversion_task = from_json_to_csv(input_path=download_task, sep=";")
    conversion_task.set_upstream(download_task)

    add_load_date_column_task = add_load_date_column(
        input_path=conversion_task, sep=";"
    )
    add_load_date_column_task.set_upstream(conversion_task)

    create_partitions_task = create_partitions(
        data_path="./data/raw", partition_directory="./data/partition_directory"
    )
    create_partitions_task.set_upstream(add_load_date_column_task)

    upload_to_datalake_task = upload_to_datalake(
        input_path="./data/partition_directory",
        dataset_id=dataset_id,
        table_id=table_id,
        if_exists="replace",
        csv_delimiter=";",
        if_storage_data_exists="replace",
        biglake_table=True,
    )
    upload_to_datalake_task.set_upstream(create_partitions_task)

    # Trigger DBT flow run
    with case(materialize_after_dump, True):
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "mode": materialization_mode,
                "dbt_alias": True,
                "materialize_to_datario": materialize_to_datario,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id}",
        )

        materialization_flow.set_upstream(upload_to_datalake_task)

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


dump_vitai_posicao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitai_posicao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
)

dump_vitai_posicao.schedule = every_day_at_six_am


with Flow(
    name="SMS: Dump Vitai - Captura Movimentos de Estoque", code_owners=["thiago"]
) as dump_vitai_movimentos:
    # Set Parameters
    # Vitai
    date = Parameter("date", default=None)
    #  Vault
    vault_path = vitai_constants.VAULT_PATH.value
    vault_key = vitai_constants.VAULT_KEY.value
    #  GCP
    dataset_id = vitai_constants.DATASET_ID.value
    table_id = vitai_constants.TABLE_MOVIMENTOS_ID.value

    # Start run
    create_folders_task = create_folders()

    build_date_task = build_movimentos_date(date_param=date)
    build_date_task.set_upstream(create_folders_task)

    build_url_task = build_movimentos_url(date_param=date)
    build_url_task.set_upstream(build_date_task)

    download_task = download_from_api(
        url=build_url_task,
        params=None,
        file_folder="./data/raw",
        file_name=table_id,
        vault_path=vault_path,
        vault_key=vault_key,
        add_load_date_to_filename=True,
        load_date=build_date_task,
    )
    download_task.set_upstream(build_url_task)

    conversion_task = from_json_to_csv(input_path=download_task, sep=";")
    conversion_task.set_upstream(download_task)

    add_load_date_column_task = add_load_date_column(
        input_path=conversion_task, sep=";", load_date=build_date_task
    )
    add_load_date_column_task.set_upstream(conversion_task)

    create_partitions_task = create_partitions(
        data_path="./data/raw", partition_directory="./data/partition_directory"
    )
    create_partitions_task.set_upstream(add_load_date_column_task)

    upload_to_datalake_task = upload_to_datalake(
        input_path="./data/partition_directory",
        dataset_id=dataset_id,
        table_id=table_id,
        if_exists="replace",
        csv_delimiter=";",
        if_storage_data_exists="replace",
        biglake_table=True,
    )
    upload_to_datalake_task.set_upstream(create_partitions_task)


dump_vitai_movimentos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitai_movimentos.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
)

dump_vitai_movimentos.schedule = every_day_at_six_am
