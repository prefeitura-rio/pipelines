# -*- coding: utf-8 -*-
"""
WhatsApp Vitacare flow definition
"""
import pandas as pd
from prefect import Flow
from prefect.storage import GCS
from pipelines.constants import constants
from prefect.run_configs import KubernetesRun
from pipelines.rj_sms.utils import upload_to_datalake
from pipelines.rj_sms.dump_api_prontuario_vitacare.tasks import (
    get_patients,
    save_patients
)

from pipelines.rj_sms.dump_api_prontuario_vitacare.schedules import (
    every_day_at_six_am,
    every_day_at_seven_am
)

with Flow(
   "SMS: Dump VitaCare - Captura dos pacientes agendados"
) as dump_vitacare_scheduled_patients:
    # Tasks
    result = get_patients('6688152', context = 'scheduled')
    save = save_patients(result, context = 'scheduled')
    save.set_upstream(result)
    upload_to_datalake_task = upload_to_datalake(
        input_path=f"pipelines/rj_sms/dump_api_prontuario_vitacare/data_partition",
        dataset_id="brutos_prontuario_vitacare",
        table_id="pacientes_agendados_3_dias",
        if_exists="replace",
        csv_delimiter=";",
        if_storage_data_exists="replace",
        biglake_table=True,
    )
    upload_to_datalake_task.set_upstream(save)

dump_vitacare_scheduled_patients.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitacare_scheduled_patients.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

dump_vitacare_scheduled_patients.schedule = every_day_at_six_am

with Flow(
    "SMS: Dump VitaCare - Captura dos pacientes atendidos"
) as dump_vitacare_attended_patients:
    # Tasks
    result = get_patients('6688152', context = 'attended')
    save = save_patients(result, context = 'attended')
    save.set_upstream(result)
    upload_to_datalake_task = upload_to_datalake(
        input_path=f"pipelines/rj_sms/dump_api_prontuario_vitacare/data_partition",
        dataset_id="brutos_prontuario_vitacare",
        table_id="pacientes_atendidos_dia_anterior",
        if_exists="replace",
        csv_delimiter=";",
        if_storage_data_exists="replace",
        biglake_table=True,
    )
    upload_to_datalake_task.set_upstream(save)

dump_vitacare_attended_patients.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitacare_attended_patients.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

dump_vitacare_attended_patients.schedule = every_day_at_seven_am