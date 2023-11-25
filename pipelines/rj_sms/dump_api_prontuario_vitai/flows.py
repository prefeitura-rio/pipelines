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
from pipelines.rj_sms.dump_api_prontuario_vitai.constants import (
    constants as vitai_constants,
)
from pipelines.rj_sms.tasks import (
    get_secret,
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
    VAULT_PATH = vitai_constants.VAULT_PATH.value
    VAULT_KEY = vitai_constants.VAULT_KEY.value
    # Paramenters for GCP
    DATASET_ID = vitai_constants.DATASET_ID.value
    TABLE_ID = vitai_constants.TABLE_POSICAO_ID.value

    # Start run
    get_secret_task = get_secret(secret_path=VAULT_PATH, secret_key=VAULT_KEY)

    create_folders_task = create_folders()
    create_folders_task.set_upstream(get_secret_task)  # pylint: disable=E1101

    download_task = download_from_api(
        url="https://apidw.vitai.care/api/dw/v1/produtos/saldoAtual",
        file_folder=create_folders_task["raw"],
        file_name=TABLE_ID,
        params=None,
        crendentials=get_secret_task,
        auth_method="bearer",
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
        partition_directory=create_folders_task["partition_directory"],
    )
    create_partitions_task.set_upstream(add_load_date_column_task)

    upload_to_datalake_task = upload_to_datalake(
        input_path=create_folders_task["partition_directory"],
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
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
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
)

dump_vitai_posicao.schedule = every_day_at_six_am


with Flow(
    name="SMS: Dump Vitai - Captura Movimentos de Estoque", code_owners=["thiago"]
) as dump_vitai_movimentos:
    # Parameters
    # Parameters for Vault
    VAULT_PATH = vitai_constants.VAULT_PATH.value
    # Paramenters for GCP
    DATASET_ID = vitai_constants.DATASET_ID.value
    TABLE_ID = vitai_constants.TABLE_MOVIMENTOS_ID.value
    # Parameters for Vitai
    DATE = Parameter("date", default=None)

    # Start run
    get_secret_task = get_secret(secret_path=VAULT_PATH, secret_key=VAULT_KEY)

    create_folders_task = create_folders()
    create_folders_task.set_upstream(get_secret_task)  # pylint: disable=E1101

    build_date_task = build_movimentos_date(date_param=DATE)
    build_date_task.set_upstream(create_folders_task)

    build_url_task = build_movimentos_url(date_param=DATE)
    build_url_task.set_upstream(build_date_task)

    download_task = download_from_api(
        url=build_url_task,
        file_folder=create_folders_task["raw"],
        file_name=TABLE_ID,
        params=None,
        crendentials=get_secret_task,
        auth_method="bearer",
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
        partition_directory=create_folders_task["partition_directory"],
    )
    create_partitions_task.set_upstream(add_load_date_column_task)

    upload_to_datalake_task = upload_to_datalake(
        input_path=create_folders_task["partition_directory"],
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
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
