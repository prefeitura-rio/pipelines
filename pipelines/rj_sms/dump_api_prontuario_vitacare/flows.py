# -*- coding: utf-8 -*-
from prefect import Parameter
from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
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
)
from pipelines.rj_sms.dump_api_prontuario_vitai.scheduler import every_day_at_six_am

with Flow(
    name="SMS: Dump VitaCare - Captura Posição de Estoque", code_owners=["thiago"]
) as dump_vitacare_posicao:
    # Set Parameters
    #  Vault
    vault_path = None
    vault_key = None
    #  GCP
    dataset_id = "dump_vitacare"
    table_id = "estoque_posicao"

    # Start run
    create_folders_task = create_folders()

    build_params_task = build_params()
    build_params_task.set_upstream(create_folders_task)

    download_task = download_from_api(
        url="http://consolidado-ap10.pepvitacare.com:8088/reports/pharmacy/stocks",
        params=build_params_task,
        file_folder="./data/raw",
        file_name=table_id,
        vault_path=vault_path,
        vault_key=vault_key,
        add_load_date_to_filename=True,
    )
    download_task.set_upstream(build_params_task)

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


dump_vitacare_posicao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitacare_posicao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

dump_vitacare_posicao.schedule = every_day_at_six_am
