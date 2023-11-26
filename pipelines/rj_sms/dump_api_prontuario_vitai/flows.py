# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Vitai healthrecord dumping flows
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from pipelines.rj_sms.dump_api_prontuario_vitai.constants import (
    constants as vitai_constants,
)
from pipelines.utils.tasks import (
    rename_current_flow_run_dataset_table,
)
from pipelines.rj_sms.tasks import (
    get_secret,
    create_folders,
    from_json_to_csv,
    download_from_api,
    add_load_date_column,
    create_partitions,
    upload_to_datalake,
)
from pipelines.rj_sms.dump_api_prontuario_vitai.tasks import build_date_param, build_url
from pipelines.rj_sms.dump_api_prontuario_vitai.schedules import every_day_at_six_am

with Flow(name="SMS: Dump Vitai - Captura ", code_owners=["thiago"]) as dump_vitai:
    # Parameters
    # Parameters for Vault
    VAULT_PATH = vitai_constants.VAULT_PATH.value
    VAULT_KEY = vitai_constants.VAULT_KEY.value

    # Vitai API
    ENDPOINT = Parameter("endpoint", required=True)
    DATE = Parameter("date", default=None)

    # Paramenters for GCP
    DATASET_ID = Parameter(
        "DATASET_ID", default=vitai_constants.DATASET_ID.value
    )  # noqa: E501
    TABLE_ID = Parameter("table_id", required=True)

    # Start run
    # TODO: Uncomment rename_flow before production
    rename_flow_task = rename_current_flow_run_dataset_table(
        prefix="SMS Dump Vitai: ", dataset_id=TABLE_ID, table_id=""
    )

    get_secret_task = get_secret(secret_path=VAULT_PATH, secret_key=VAULT_KEY)

    create_folders_task = create_folders()
    create_folders_task.set_upstream(get_secret_task)  # pylint: disable=E1101

    build_date_param_task = build_date_param(date_param=DATE)
    build_date_param_task.set_upstream(create_folders_task)

    build_url_task = build_url(endpoint=ENDPOINT, date_param=build_date_param_task)
    build_url_task.set_upstream(build_date_param_task)

    download_task = download_from_api(
        url=build_url_task,
        file_folder=create_folders_task["raw"],
        file_name=TABLE_ID,
        params=None,
        crendentials=get_secret_task,
        auth_method="bearer",
        add_load_date_to_filename=True,
        load_date=build_date_param_task,
    )
    download_task.set_upstream(create_folders_task)

    conversion_task = from_json_to_csv(input_path=download_task, sep=";")
    conversion_task.set_upstream(download_task)

    add_load_date_column_task = add_load_date_column(
        input_path=conversion_task, sep=";"
    )
    add_load_date_column_task.set_upstream(conversion_task)

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
    )
    upload_to_datalake_task.set_upstream(create_partitions_task)


dump_vitai.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitai.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
)

dump_vitai.schedule = every_day_at_six_am
