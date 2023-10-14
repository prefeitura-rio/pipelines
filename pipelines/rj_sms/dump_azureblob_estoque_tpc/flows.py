# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
TPC inventory dumping flows
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from pipelines.rj_sms.dump_azureblob_estoque_tpc.constants import constants as tpc_constants
from pipelines.rj_sms.utils import (
    download_azure_blob,
    create_folders,
    add_load_date_column,
    create_partitions,
    upload_to_datalake,
)
from pipelines.rj_sms.dump_azureblob_estoque_tpc.tasks import (
    conform_csv_to_gcp,
)
from pipelines.rj_sms.dump_azureblob_estoque_tpc.scheduler import every_day_at_six_am

with Flow(
    name="SMS: Dump TPC - Captura de dados TPC", code_owners=["thiago"]
) as dump_tpc:
    # Parameters
    # Parameters for Vault
    vault_path = tpc_constants.VAULT_PATH.value
    vault_key = tpc_constants.VAULT_KEY.value
    # Paramenters for GCP
    dataset_id = tpc_constants.DATASET_ID.value 
    table_id = tpc_constants.TABLE_POSICAO_ID.value
    # Paramerters for Azure
    container_name = tpc_constants.CONTAINER_NAME.value
    blob_path = tpc_constants.BLOB_PATH_POSICAO.value

    # Start run
    create_folders_task = create_folders()

    download_task = download_azure_blob(
        container_name=container_name,
        blob_path=blob_path,
        file_folder=create_folders_task["raw"],
        file_name=table_id,
        vault_path=vault_path,
        vault_key=vault_key,
        add_load_date_to_filename=True,
    )
    download_task.set_upstream(create_folders_task)

    conform_task = conform_csv_to_gcp(download_task)
    conform_task.set_upstream(download_task)

    add_load_date_column_task = add_load_date_column(
        input_path=download_task, sep=";"
    )
    add_load_date_column_task.set_upstream(conform_task)

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

dump_tpc.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_tpc.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

dump_tpc.schedule = every_day_at_six_am