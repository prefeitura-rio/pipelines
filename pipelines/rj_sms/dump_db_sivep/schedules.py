# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from pipelines.constants import constants
from pipelines.utils.dump_db.utils import generate_dump_db_schedules
from pipelines.utils.utils import untuple_clocks as untuple

#####################################
#
# EGPWeb Schedules
#
#####################################

sms_web_queries = {
    "srag": {
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM gtsinan.vw_sivep_escritoriodados;",
        "materialize_after_dump": False,
    }
}

sms_web_clocks = generate_dump_db_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2022, 9, 14, 15, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    db_database="gtsinan",
    db_host="10.50.74.94",
    db_port=3306,
    db_type="mysql",
    dataset_id="sms_covid",
    vault_secret_path="formacao-sivep",
    table_parameters=sms_web_queries,
)

sms_web_weekly_update_schedule = Schedule(clocks=untuple(sms_web_clocks))
