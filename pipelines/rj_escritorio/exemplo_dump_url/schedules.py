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
# EGPWeb Schedules
#
#####################################

gsheets_urls = {
    "teste_table_formacao_claurenti": {
        "dump_mode": "overwrite",
        "url": 'https://docs.google.com/spreadsheets/d/1uF-Gt5AyZmxCQQEaebvWF4ddRHeVuL6ANuoaY_-uAXE/edit#gid=0',
        "url_type": "google_sheet",
        "gsheets_sheet_name": "sheet_1",
        "materialize_after_dump": False,
        "dataset_id": "exemplo_url_dump_claurenti",
    }
}    


gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2022, 11, 8, 14, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
    table_parameters=gsheets_urls,
    dataset_id="exemplo_url_dump_claurenti",
)

gsheets_daily_update_schedule = Schedule(clocks=untuple(gsheets_clocks))