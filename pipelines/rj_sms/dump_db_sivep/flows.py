# -*- coding: utf-8 -*-
"""
Database dumping flows for sms project SIVEP
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_sms.dump_db_sivep.schedules import sms_web_weekly_update_schedule
from pipelines.utils.dump_db.flows import dump_sql_flow
from pipelines.utils.utils import set_default_parameters

sms_sivep_flow = deepcopy(dump_sql_flow)
sms_sivep_flow.name = "SMS: SIVEP - Ingerir tabelas de banco SQL"
sms_sivep_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_sivep_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
)

sms_default_parameters = {
    "db_database": "gtsinan",
    "db_host": "10.50.74.94",
    "db_port": 3306,
    "db_type": "mysql",
    "vault_secret_path": "formacao-sivep",
    "dataset_id": "sms_covid",
}
sms_sivep_flow = set_default_parameters(
    sms_sivep_flow, default_parameters=sms_default_parameters
)

sms_sivep_flow.schedule = sms_web_weekly_update_schedule
