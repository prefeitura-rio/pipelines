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
    download_api,
    set_destination_file_path,
    fix_payload_tpc,
    fix_payload_vitai,
)

with Flow(
    name="SMS: Farmacia - Captura de dados TPC", code_owners=["thiago", "andre"]
) as captura_tpc:
    # Set Parameters
    #  Azure
    container_name = Parameter("container_name", default="datalaketpc")
    blob_path = Parameter(
        "blob_path",
        default="gold/logistico/cliente=prefeitura_rio/planta=sms_rio/estoque_local/",
    )
    blob_name = Parameter("blob_name", default="estoque_local.csv")

    #  GCP
    dataset_id = "estoque"
    table_id = "tpc"
    dump_mode = "append"  # append or overwrite

    # Start run
    file_path_task = set_destination_file_path(blob_name)

    download_task = download_azure_blob(
        container_name, blob_path + blob_name, file_path_task
    )
    download_task.set_upstream(file_path_task)

    fix_payload_task = fix_payload_tpc(file_path_task)
    fix_payload_task.set_upstream(download_task)

    upload_task = create_table_and_upload_to_gcs(
        data_path=file_path_task,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=True,
        wait=None,
    )
    upload_task.set_upstream(fix_payload_task)

captura_tpc.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
captura_tpc.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

with Flow(
    name="SMS: Farmacia - Captura de dados Vitai", code_owners=["thiago", "andre"]
) as captura_vitai:
    
    # Set Parameters
    file_name = Parameter("file_name", default="estoque.csv")
    source = Parameter("source", default="vitai")
    #  GCP
    dataset_id = "estoque"
    table_id = "vitai"
    dump_mode = "append"  # append or overwrite

    # Start run
    file_path_task = set_destination_file_path(file_name)

    download_task = download_api(
        url="https://apidw.vitai.care/api/dw/v1/produtos/saldoAtual",
        destination_file_path=file_path_task,
        source = source)
    download_task.set_upstream(file_path_task)

    fix_payload_task = fix_payload_vitai(file_path_task)
    fix_payload_task.set_upstream(download_task)


    upload_task = create_table_and_upload_to_gcs(
        data_path=file_path_task,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=True,
        wait=None,
    )
    upload_task.set_upstream(fix_payload_task)


captura_vitai.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
captura_vitai.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)