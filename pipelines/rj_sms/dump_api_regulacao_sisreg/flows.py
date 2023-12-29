# -*- coding: utf-8 -*-
"""
WhatsApp SISREG flow definition
"""
from prefect import Flow
from prefect.storage import GCS
from pipelines.constants import constants
from prefect.run_configs import KubernetesRun
from pipelines.rj_sms.tasks import upload_to_datalake
from pipelines.rj_sms.dump_api_regulacao_sisreg.tasks import get_patients, save_patients
from pipelines.rj_sms.dump_api_regulacao_sisreg.schedules import every_day_at_six_am

with Flow(
    "SMS: Dump SISREG - Captura dos pacientes agendados"
) as dump_sisreg_scheduled_patients:
    # Tasks
    dataframe = get_patients("6688152")
    save = save_patients(dataframe)
    save.set_upstream(dataframe)
    upload_to_datalake_task = upload_to_datalake(
        input_path=f"pipelines/rj_sms/dump_api_regulacao_sisreg/data_partition",
        dataset_id="brutos_regulacao_sisreg",
        table_id="pacientes_agendados_5_dias",
        if_exists="replace",
        csv_delimiter=";",
        if_storage_data_exists="replace",
        biglake_table=True,
    )
    upload_to_datalake_task.set_upstream(save)

dump_sisreg_scheduled_patients.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_sisreg_scheduled_patients.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

dump_sisreg_scheduled_patients.schedule = every_day_at_six_am
