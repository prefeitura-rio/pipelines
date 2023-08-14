# -*- coding: utf-8 -*-
from prefect import Parameter, Flow
from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
)
from pipelines.rj_sms.farmacia_estoque.tasks import (
    download_azure_blob,
    set_destination_path,
    list_blobs_after_time,
)
from datetime import datetime

with Flow(
    name="SMS: Farmacia - Captura de dados TPC", code_owners=["thiago", "andre"]
) as captura_tpc:

    # Set Parameters
    #  Azure
    container_name = Parameter("container_name", default="tpc")
    blob_name = Parameter("blob_name", default="report.csv")

    #  GCP
    # TODO: passar como parâmetros
    dataset_id = "estoque"
    table_id = "tpc_current"
    dump_mode = "append"  # append or overwrite

    # Start run
    file_path_task = set_destination_path(blob_name)

    download_task = download_azure_blob(container_name, blob_name, file_path_task)
    download_task.set_upstream(file_path_task)

    upload_task = create_table_and_upload_to_gcs(
        data_path=file_path_task,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=True,
        wait=None,
    )
    upload_task.set_upstream(download_task)

captura_tpc.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
captura_tpc.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

# with Flow(
#    name="SMS: Farmacia - Lista dados Azure",
#    code_owners=["thiago", "andre"]
# ) as lista_blob:
#    # Replace these values with your own
#    container_name = "tpc"
#    after_time = datetime(2023, 8, 11)  # Replace with your desired time
#
#    blob_list = list_blobs_after_time(container_name, after_time)
#
# lista_blob.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# lista_blob.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
