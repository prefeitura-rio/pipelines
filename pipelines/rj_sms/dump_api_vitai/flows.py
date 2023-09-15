# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.tasks.control_flow.conditional import ifelse
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
    upload_to_datalake)
from pipelines.rj_sms.dump_api_vitai.tasks import (
    build_movimentos_date, build_movimentos_url, is_none
)
from pipelines.rj_sms.scheduler import every_day_at_six_am

with Flow(
    name="SMS: Dump Vitai - Captura Posição de Estoque", code_owners=["thiago"]
) as dump_vitai_posicao:
    # Set Parameters
    #  Vault
    vault_path = "estoque_vitai"
    vault_key = "token"
    #  GCP
    dataset_id = "dump_vitai"
    table_id = "estoque_posicao_new"

    # Start run
    create_folders_task = create_folders()

    download_task = download_from_api(
        url = "https://apidw.vitai.care/api/dw/v1/produtos/saldoAtual",
        params = None,
        file_folder = "./data/raw",
        file_name = table_id,
        vault_path = vault_path,
        vault_key = vault_key,
        add_load_date_to_filename = True,
    )
    download_task.set_upstream(create_folders_task)

    conversion_task = from_json_to_csv(
        input_path=download_task,
          sep=";")
    conversion_task.set_upstream(download_task)

    add_load_date_column_task = add_load_date_column(
        input_path=conversion_task,
        sep = ";")	
    add_load_date_column_task.set_upstream(conversion_task) 

    create_partitions_task = create_partitions(
        data_path =  "./data/raw", 
        partition_directory = "./data/partition_directory")
    create_partitions_task.set_upstream(add_load_date_column_task)
    
    upload_to_datalake_task = upload_to_datalake(
        input_path="./data/partition_directory",
        dataset_id=dataset_id,
        table_id=table_id,
        if_exists= "replace",
        csv_delimiter= ";",
        if_storage_data_exists= "replace",
        biglake_table= True)
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
    # Set Parameters
    # Vitai
    date = Parameter("date", default=None)
    #  Vault
    vault_path = "estoque_vitai"
    vault_key = "token"
    #  GCP
    dataset_id = "dump_vitai"
    table_id = "estoque_movimentos_new"

    # Start run
    create_folders_task = create_folders()
    create_folders_task.set_upstream(date)

    build_date_task = build_movimentos_date(date_param = date)
    build_date_task.set_upstream(create_folders_task)

    build_url_task = build_movimentos_url(date_param = date)
    build_url_task.set_upstream(build_date_task)


    download_task = download_from_api(
        url = build_url_task,
        params = None,
        file_folder = "./data/raw",
        file_name = table_id,
        vault_path = vault_path,
        vault_key = vault_key,
        add_load_date_to_filename = True,
        load_date = build_date_task
    )
    download_task.set_upstream(build_url_task)

    #conversion_task = from_json_to_csv(
    #    input_path=download_task,
    #      sep=";")
    #conversion_task.set_upstream(download_task)
#
    #add_load_date_column_task = add_load_date_column(
    #    input_path=conversion_task,
    #    sep = ";",
    #    load_date = build_movimentos_args["date"])	
    #add_load_date_column_task.set_upstream(conversion_task) 
#
    #create_partitions_task = create_partitions(
    #    data_path =  "./data/raw", 
    #    partition_directory = "./data/partition_directory")
    #create_partitions_task.set_upstream(add_load_date_column_task)
    #
    #upload_to_datalake_task = upload_to_datalake(
    #    input_path="./data/partition_directory",
    #    dataset_id=dataset_id,
    #    table_id=table_id,
    #    if_exists= "replace",
    #    csv_delimiter= ";",
    #    if_storage_data_exists= "replace",
    #    biglake_table= True)
    #upload_to_datalake_task.set_upstream(create_partitions_task)    


dump_vitai_movimentos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitai_movimentos.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

dump_vitai_movimentos.schedule = every_day_at_six_am