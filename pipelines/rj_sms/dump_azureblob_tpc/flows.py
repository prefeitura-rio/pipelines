# -*- coding: utf-8 -*-
from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
)
from pipelines.rj_sms.utils import (
    download_azure_blob,
    set_destination_file_path,
)

from pipelines.rj_sms.dump_azureblob_tpc.tasks import (
    conform_csv_to_gcp,
)

# TODO: mudar método de passar o path do arquivo. Deixar igual vitai
with Flow(
    name="SMS: Dump TPC - Captura de dados TPC", code_owners=["thiago"]
) as dump_tpc:
    # Set Parameters
    #  Vault
    vault_path = "estoque_tpc"
    vault_token = "credential"
    #  Azure
    container_name = "datalaketpc"
    blob_folder_path = (
        "gold/logistico/cliente=prefeitura_rio/planta=sms_rio/estoque_local/"
    )
    blob_file_name = "estoque_local.csv"
    #  GCP
    dataset_id = "dump_tpc"
    table_id = "estoque_posicao"
    dump_mode = "append"  # append or overwrite

    # Start run
    file_path_task = set_destination_file_path(blob_file_name)

    download_task = download_azure_blob(
        container_name=container_name,
        blob_path=blob_folder_path + blob_file_name,
        destination_file_path=file_path_task,
        vault_path=vault_path,
        vault_token=vault_token,
    )
    download_task.set_upstream(file_path_task)

    conform_task = conform_csv_to_gcp(file_path_task)
    conform_task.set_upstream(download_task)

    upload_task = create_table_and_upload_to_gcs(
        data_path=file_path_task,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=True,
        wait=None,
    )
    upload_task.set_upstream(conform_task)

dump_tpc.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_tpc.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)
