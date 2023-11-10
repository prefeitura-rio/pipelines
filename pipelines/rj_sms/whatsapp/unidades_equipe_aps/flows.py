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
    read_file, 
    save_file,
    upload_to_datalake
)

with Flow("Unidades Equipe APS") as flow_unidades_equipe_aps:

    # Tasks
    dataframe = read_file()
    save = save_file(dataframe)
    save.set_upstream(dataframe)
    upload_to_datalake_task = upload_to_datalake(
        input_path=f"pipelines/rj_sms/whatsapp/unidades_equipe_aps/data_partition",
        dataset_id="whatsapp",
        table_id="unidades_equipe_aps",
        if_exists="replace",
        csv_delimiter=";",
        if_storage_data_exists="replace",
        biglake_table=True,
    )
    upload_to_datalake_task.set_upstream(save)

flow_unidades_equipe_aps.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_unidades_equipe_aps.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)
