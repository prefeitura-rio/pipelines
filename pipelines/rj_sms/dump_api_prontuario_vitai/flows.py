# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Vitai healthrecord dumping flows
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from pipelines.rj_sms.dump_api_prontuario_vitai.constants import constants as vitai_constants
from pipelines.rj_sms.utils import (
    create_folders,
    from_json_to_csv,
    download_from_api,
    add_load_date_column,
    create_partitions,
    upload_to_datalake,
)
from pipelines.rj_sms.dump_api_prontuario_vitai.tasks import (
    build_movimentos_date,
    build_movimentos_url,
)
from pipelines.rj_sms.dump_api_prontuario_vitai.schedules import every_day_at_six_am


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

    # Start run
    create_folders_task = create_folders()

    download_task = download_from_api(
        url="https://apidw.vitai.care/api/dw/v1/produtos/saldoAtual",
        params=None,
        file_folder=create_folders_task["raw"],
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
        data_path=create_folders_task["raw"],
        partition_directory=create_folders_task["partition_directory"]
    )
    create_partitions_task.set_upstream(add_load_date_column_task)

    upload_to_datalake_task = upload_to_datalake(
        input_path=create_folders_task["partition_directory"],
        dataset_id=dataset_id,
        table_id=table_id,
        if_exists="replace",
        csv_delimiter=";",
        if_storage_data_exists="replace",
        biglake_table=True,
    )
    upload_to_datalake_task.set_upstream(create_partitions_task)


dump_vitai_posicao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitai_posicao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

dump_vitai_posicao.schedule = every_day_at_six_am


with Flow(
    name="SMS: Dump Vitai - Captura Movimentos de Estoque", code_owners=["thiago"]
) as dump_vitai_movimentos:
    # Parameters
    # Parameters for Vault
    vault_path = vitai_constants.VAULT_PATH.value
    vault_key = vitai_constants.VAULT_KEY.value
    # Paramenters for GCP
    dataset_id = vitai_constants.DATASET_ID.value
    table_id = vitai_constants.TABLE_MOVIMENTOS_ID.value
    # Parameters for Vitai
    date = Parameter("date", default=None)

    # Start run
    create_folders_task = create_folders()

    build_date_task = build_movimentos_date(date_param=date)
    build_date_task.set_upstream(create_folders_task)

    build_url_task = build_movimentos_url(date_param=date)
    build_url_task.set_upstream(build_date_task)

    download_task = download_from_api(
        url=build_url_task,
        params=None,
        file_folder=create_folders_task["raw"],
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
        data_path=create_folders_task["raw"],
        partition_directory=create_folders_task["partition_directory"]
    )
    create_partitions_task.set_upstream(add_load_date_column_task)

    upload_to_datalake_task = upload_to_datalake(
        input_path=create_folders_task["partition_directory"],
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
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

dump_vitai_movimentos.schedule = every_day_at_six_am
