# -*- coding: utf-8 -*-
"""
WhatsApp flow definition
"""
import pandas as pd
from prefect import Flow
from prefect.storage import GCS
from pipelines.constants import constants
from prefect.run_configs import KubernetesRun
from pipelines.rj_sms.utils import upload_to_datalake
from pipelines.rj_sms.whatsapp.sisreg_scheduled_patients.tasks import (
    get_patients,
    save_patients,
)

with Flow(
    "SMS: Dump VitaCare - Captura dos pacientes agendados"
) as flow_clinica_scheduled_patients:
    # Tasks
    result = get_patients()
    save = save_patients(result)
    save.set_upstream(result)
    upload_to_datalake_task = upload_to_datalake(
        input_path=f"pipelines/rj_sms/whatsapp/clinica_scheduled_patients/data_partition",
        dataset_id="whatsapp",
        table_id="clinica_scheduled_patients",
        if_exists="replace",
        csv_delimiter=";",
        if_storage_data_exists="replace",
        biglake_table=True,
    )
    upload_to_datalake_task.set_upstream(save)

flow_clinica_scheduled_patients.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_clinica_scheduled_patients.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)
