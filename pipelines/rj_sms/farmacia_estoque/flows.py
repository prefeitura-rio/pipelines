# -*- coding: utf-8 -*-
from prefect import Parameter, Flow

# from pipelines.utils.decorators import Flow
from pipelines.utils.utils import (
    get_vault_secret,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
)
from pipelines.rj_sms.farmacia_estoque.tasks import (
    download_azure_blob,
)
import os


with Flow(
    name="SMS: Farmacia - Captura de dados TPC",
) as captura_tpc:

    # Get crendetials
    connection_string = get_vault_secret(secret_path="estoque_tpc")["data"][
        "connection_string"
    ]

    # Set Parameters
    #  Azure
    container_name = Parameter("container_name", default="tpc")
    blob_name = Parameter("blob_name", default="report.csv")
    destination_folder_path = os.path.expanduser("~")
    #  GCP
    dataset_id = "estoque"
    table_id = "tpc_current"
    dump_mode = "append"  # append or overwrite

    # Declare tasks
    download_task = download_azure_blob(
        connection_string, container_name, blob_name, destination_folder_path
    )

    data_path = destination_folder_path + "/report.csv"

    upload_task = create_table_and_upload_to_gcs(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=True,
        wait=None,
    )

    # Set taks dependencies
    download_task.set_downstream(upload_task)
