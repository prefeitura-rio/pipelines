# -*- coding: utf-8 -*-
from prefect import Parameter, Flow
from prefect.tasks.core.operators import GetAttr

# from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
)
from pipelines.rj_sms.farmacia_estoque.tasks import (
    download_azure_blob,
    list_blobs_after_time,
)
import os
from datetime import datetime

with Flow(
    name="SMS: Farmacia - Captura de dados TPC",
) as captura_tpc:

    # Set Parameters
    #  Azure
    container_name = Parameter("container_name", default="tpc")
    blob_name = Parameter("blob_name", default="report.csv")
    destination_folder_path = os.path.expanduser("~")
    #  GCP
    # TODO: passar como parâmetros
    dataset_id = "estoque"
    table_id = "tpc_current"
    dump_mode = "append"  # append or overwrite

    # Declare tasks
    download_task = download_azure_blob(container_name, blob_name, destination_folder_path)

    # TODO: ler o nome do arquivo a partir do parâmtro blob_name
    data_path = destination_folder_path + "/report.csv"

    upload_task = create_table_and_upload_to_gcs(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=True,
        wait=None,
    )
    upload_task.set_upstream(download_task)

captura_tpc.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
captura_tpc.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)

with Flow("Lista Arquivos") as lista_blob:
    # Replace these values with your own
    container_name = "tpc"
    after_time = datetime(2023, 8, 11)  # Replace with your desired time

    blob_list = list_blobs_after_time(container_name, after_time)

lista_blob.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
lista_blob.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)

