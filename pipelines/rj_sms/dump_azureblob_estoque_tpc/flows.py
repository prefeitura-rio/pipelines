# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
TPC inventory dumping flows
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from pipelines.rj_sms.dump_azureblob_estoque_tpc.constants import (
    constants as tpc_constants,
)
from pipelines.utils.tasks import (
    rename_current_flow_run_dataset_table,
)
from pipelines.rj_sms.tasks import (
    get_secret,
    download_azure_blob,
    create_folders,
    add_load_date_column,
    create_partitions,
    upload_to_datalake,
)
from pipelines.rj_sms.dump_azureblob_estoque_tpc.tasks import (
    get_blob_path,
    conform_csv_to_gcp,
)
from pipelines.rj_sms.dump_azureblob_estoque_tpc.schedules import (
    tpc_daily_update_schedule,
)

with Flow(
    name="SMS: Dump TPC - Ingerir dados do estoque TPC", code_owners=["thiago"]
) as dump_tpc:
    #####################################
    # Parameters
    #####################################

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=True)

    # Vault
    VAULT_PATH = tpc_constants.VAULT_PATH.value
    VAULT_KEY = tpc_constants.VAULT_KEY.value

    # TPC Azure
    CONTAINER_NAME = tpc_constants.CONTAINER_NAME.value
    BLOB_FILE = Parameter("blob_file", required=True)

    # GCP
    DATASET_ID = Parameter("DATASET_ID", default=tpc_constants.DATASET_ID.value)
    TABLE_ID = Parameter("table_id", required=True)

    #####################################
    # Rename flow run
    ####################################

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run_dataset_table(
            prefix="SMS Dump TPC: ", dataset_id=TABLE_ID, table_id=""
        )

    ####################################
    # Tasks section #1 - Get data
    #####################################

    get_secret_task = get_secret(secret_path=VAULT_PATH, secret_key=VAULT_KEY)

    create_folders_task = create_folders()
    create_folders_task.set_upstream(get_secret_task)  # pylint: disable=E1101

    get_blob_path_task = get_blob_path(blob_file=BLOB_FILE)
    get_blob_path_task.set_upstream(create_folders_task)

    download_task = download_azure_blob(
        container_name=CONTAINER_NAME,
        blob_path=get_blob_path_task,
        file_folder=create_folders_task["raw"],
        file_name=TABLE_ID,
        credentials=get_secret_task,
        add_load_date_to_filename=True,
    )
    download_task.set_upstream(create_folders_task)

    #####################################
    # Tasks section #2 - Transform data and Create table
    #####################################

    conform_task = conform_csv_to_gcp(filepath=download_task, blob_file=BLOB_FILE)
    conform_task.set_upstream(download_task)

    add_load_date_column_task = add_load_date_column(input_path=download_task, sep=";")
    add_load_date_column_task.set_upstream(conform_task)

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
        dataset_is_public=False,
    )
    upload_to_datalake_task.set_upstream(create_partitions_task)

dump_tpc.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_tpc.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

dump_tpc.schedule = tpc_daily_update_schedule
