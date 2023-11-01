# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from pipelines.rj_sms.dump_api_prontuario_vitacare.constants import (
    constants as vitacare_constants,
)
from pipelines.rj_sms.utils import (
    create_folders,
    from_json_to_csv,
    download_from_api,
    add_load_date_column,
    create_partitions,
    upload_to_datalake,
)
from pipelines.rj_sms.dump_api_prontuario_vitacare.tasks import (
    build_params,
    download_multiple_files,
    download_to_cloudstorage
    )

from pipelines.rj_sms.dump_api_prontuario_vitai.schedules import every_day_at_six_am


with Flow( name="SMS: Dump VitaCare - Captura Posição de Estoque", code_owners=["thiago"] ) as dump_vitacare_posicao:  # noqa: E501
    # Set Parameters
    #  Vault
    vault_path = vitacare_constants.VAULT_PATH.value
    vault_key = vitacare_constants.VAULT_KEY.value
    #  GCP
    dataset_id = vitacare_constants.DATASET_ID.value
    table_id = vitacare_constants.TABLE_POSICAO_ID.value

    # Vitacare API
    endpoint_base_url = vitacare_constants.ENDPOINT_BASE_URL.value
    endpoint_posicao = vitacare_constants.ENDPOINT_POSICAO.value
    date = Parameter("date", default="today")

    # Start run
    create_folders_task = create_folders()

    build_params_task = build_params(date_param=date)
    build_params_task.set_upstream(create_folders_task)

    download_multiple_files_task = download_multiple_files(
        base_urls=endpoint_base_url,
        endpoint=endpoint_posicao,
        params=build_params_task,
        table_id=table_id,
        vault_path=vault_path,
        vault_key=vault_key)
    download_multiple_files_task.set_upstream(build_params_task)

    create_partitions_task = create_partitions(
        data_path="./data/raw", partition_directory="./data/partition_directory"
    )
    create_partitions_task.set_upstream(download_multiple_files_task)

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


dump_vitacare_posicao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitacare_posicao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

dump_vitacare_posicao.schedule = every_day_at_six_am


with Flow(name="SMS: Dump VitaCare - Captura Posição de Estoque", code_owners=["thiago"]) as dump_vitacare_movimento:  # noqa: E501
    # Set Parameters
    #  Vault
    vault_path = vitacare_constants.VAULT_PATH.value
    vault_key = vitacare_constants.VAULT_KEY.value
    #  GCP
    dataset_id = vitacare_constants.DATASET_ID.value
    table_id = vitacare_constants.TABLE_MOVIMENTOS_ID.value

    # Vitacare API
    endpoint_base_url = vitacare_constants.ENDPOINT_BASE_URL.value
    endpoint_movimentos = vitacare_constants.ENDPOINT_MOVIMENTOS.value
    date = Parameter("date", default="yesterday")

    # Start run
    create_folders_task = create_folders()

    build_params_task = build_params(date_param=date)
    build_params_task.set_upstream(create_folders_task)

    download_multiple_files_task = download_multiple_files(
        base_urls=endpoint_base_url,
        endpoint=endpoint_movimentos,
        params=build_params_task,
        table_id=table_id,
        vault_path=vault_path,
        vault_key=vault_key)
    download_multiple_files_task.set_upstream(build_params_task)

    create_partitions_task = create_partitions(
        data_path="./data/raw", partition_directory="./data/partition_directory"
    )
    create_partitions_task.set_upstream(download_multiple_files_task)

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


dump_vitacare_movimento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitacare_movimento.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

dump_vitacare_movimento.schedule = every_day_at_six_am


with Flow(
    name="SMS: Dump VitaCare - Captura Posição de Estoque", code_owners=["thiago"]
) as dump_vitacare_posicao2:
    # Set Parameters
    #  Vault
    vault_path = vitacare_constants.VAULT_PATH.value
    vault_key = vitacare_constants.VAULT_KEY.value
    #  GCP
    dataset_id = vitacare_constants.DATASET_ID.value
    table_id = vitacare_constants.TABLE_POSICAO_ID.value

    # Vitacare API
    endpoint_base_url = vitacare_constants.ENDPOINT_BASE_URL.value
    endpoint_posicao = vitacare_constants.ENDPOINT_POSICAO.value
    date = Parameter("date", default="today")

    # Start run
    create_folders_task = create_folders()

    download_to_cloudstorage_task = download_to_cloudstorage()
    download_to_cloudstorage_task.set_upstream(create_folders_task)