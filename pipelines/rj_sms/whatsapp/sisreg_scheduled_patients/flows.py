# -*- coding: utf-8 -*-
"""
WhatsApp flow definition
"""


from prefect import Flow
from prefect.storage import GCS
from pipelines.constants import constants
from prefect.run_configs import KubernetesRun
from datetime import datetime, timedelta

from tasks import (
    get_patients, 
    save_patients,
    upload_to_datalake
)

with Flow("SISREG patients") as flow_sisreg_scheduled_patients:

    # Tasks
    dataframe = get_patients()
    save = save_patients(dataframe)
    save.set_upstream(dataframe)
    upload_to_datalake_task = upload_to_datalake(
        input_path=f"pipelines/rj_sms/whatsapp/sisreg_scheduled_patients/data_partition",
        dataset_id="whatsapp",
        table_id="sisreg_scheduled_patients",
        if_exists="replace",
        csv_delimiter=";",
        if_storage_data_exists="replace",
        biglake_table=True,
    )
    upload_to_datalake_task.set_upstream(save)

flow_sisreg_scheduled_patients.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_sisreg_scheduled_patients.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)
