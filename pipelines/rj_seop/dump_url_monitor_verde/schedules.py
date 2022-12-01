# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from pipelines.constants import constants
from pipelines.utils.dump_url.utils import generate_dump_url_schedules
from pipelines.utils.utils import untuple_clocks as untuple

#####################################
#
# Monitor Verde Schedule
#
#####################################

gsheets_urls = {
    "alertas_desmatamento": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/1Q5MbIStFAWzM1q9Vf82xzKBYAZHNt9l6/view?usp=share_link",
        "url_type": "google_drive",
        "materialize_after_dump": True,
    }
}

gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(days=365),
    start_date=datetime(2022, 11, 4, 20, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SEOP_AGENT_LABEL.value,
    ],
    dataset_id="conservacao_ambiental_monitor_verde",
    table_parameters=gsheets_urls,
)

gsheets_daily_update_schedule = Schedule(clocks=untuple(gsheets_clocks))
