# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Vitacare healthrecord dumping flows
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from pipelines.rj_sms.dump_api_prontuario_vitacare.constants import (
    constants as vitacare_constants,
)
from pipelines.rj_sms.tasks import (
    get_secret,
    create_folders,
    cloud_function_request,
    create_partitions,
    upload_to_datalake,
)
from pipelines.rj_sms.dump_api_prontuario_vitacare.tasks import (
    rename_flow,
    build_url,
    build_params,
    create_filename,
    save_data_to_file,
)

from pipelines.rj_sms.dump_api_prontuario_vitacare.schedules import (
    vitacare_daily_update_schedule,
)


with Flow(
    name="SMS: Dump VitaCare - Ingerir dados do prontuário VitaCare",
    code_owners=["thiago", "andre",],
) as dump_vitacare:
    #####################################
    # Parameters
    #####################################

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=True)

    #  Vault
    VAULT_PATH = vitacare_constants.VAULT_PATH.value

    # Vitacare API
    AP = Parameter("ap", required=True, default="10")
    ENDPOINT = Parameter("endpoint", required=True)
    DATE = Parameter("date", default="today")

    #  GCP
    DATASET_ID = Parameter("dataset_id", default=vitacare_constants.DATASET_ID.value)
    TABLE_ID = Parameter("table_id", required=True)

    #####################################
    # Rename flow run
    ####################################

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_flow(table_id=TABLE_ID, ap=AP)

    ####################################
    # Tasks section #1 - Get data
    #####################################

    get_secret_task = get_secret(secret_path=VAULT_PATH)

    create_folders_task = create_folders()
    create_folders_task.set_upstream(get_secret_task)  # pylint: disable=E1101

    build_url_task = build_url(ap=AP, endpoint=ENDPOINT)

    build_params_task = build_params(date_param=DATE)
    build_params_task.set_upstream(create_folders_task)  # pylint: disable=E1101

    file_name_task = create_filename(table_id=TABLE_ID, ap=AP)
    file_name_task.set_upstream(build_params_task)

    download_task = cloud_function_request(
        url=build_url_task,
        credential=get_secret_task,
        request_type="GET",
        body_params=None,
        query_params=build_params_task,
        env="prod",
    )
    download_task.set_upstream(file_name_task)  # pylint: disable=E1101

    save_data_task = save_data_to_file(
        data=download_task,
        file_folder=create_folders_task["raw"],
        table_id=TABLE_ID,
        ap=AP,
        add_load_date_to_filename=True,
        load_date=build_params_task["date"],
    )
    save_data_task.set_upstream(download_task)  # pylint: disable=E1101

    #####################################
    # Tasks section #2 - Transform data and Create table
    #####################################

    with case(save_data_task, True):
        create_partitions_task = create_partitions(
            data_path=create_folders_task["raw"],
            partition_directory=create_folders_task["partition_directory"],
        )
        create_partitions_task.set_upstream(save_data_task)

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


dump_vitacare.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitacare.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
)

dump_vitacare.schedule = vitacare_daily_update_schedule
