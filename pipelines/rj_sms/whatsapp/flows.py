# -*- coding: utf-8 -*-
"""
WhatsApp flow definition
"""
import pandas as pd
from prefect import Flow
from prefect.storage import GCS
#from pipelines.constants import constants
from prefect.run_configs import KubernetesRun

GCS_FLOWS_BUCKET = "datario-public"
# Docker image
DOCKER_TAG = "AUTO_REPLACE_DOCKER_TAG"
DOCKER_IMAGE_NAME = "AUTO_REPLACE_DOCKER_IMAGE"
DOCKER_IMAGE = f"{DOCKER_IMAGE_NAME}:{DOCKER_TAG}"
RJ_SMS_DEV_AGENT_LABEL = "rj-sms-dev"

from tasks import (
    get_patients, 
    save_patients
)

from utils import (
    log,
    upload_to_datalake,
)

with Flow("SISREG patients") as flow_whatsapp:

    # Tasks
    #dataframe = get_patients()
    #dataframe = pd.DataFrame()
    upload_to_datalake_task = upload_to_datalake(
        input_path="/Users/andremartins/Documents/GitHub/pipelines/pipelines/rj_sms/whatsapp/files/2023-10-21.csv",
        dataset_id="whatsapp",
        table_id="sisreg_scheduled_patients",
        if_exists="replace",
        csv_delimiter=";",
        if_storage_data_exists="replace",
        biglake_table=True,
    )

flow_whatsapp.storage = GCS(GCS_FLOWS_BUCKET)
flow_whatsapp.run_config = KubernetesRun(
    image=DOCKER_IMAGE,
    labels=[
        RJ_SMS_DEV_AGENT_LABEL,
    ],
)
