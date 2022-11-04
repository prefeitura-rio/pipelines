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
    "test_table_1": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1uF-Gt5AyZmxCQQEaebvWF4ddRHeVuL6ANuoaY_-uAXE\
            /edit#gid=0",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "sheet_1",
    },
    "test_table_2": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1uF-Gt5AyZmxCQQEaebvWF4ddRHeVuL6ANuoaY_-uAXE\
            /edit#gid=0",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "sheet_2",
        "gsheets_sheet_range": "B3:D7",
    },
}

gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2022, 10, 20, 10, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
    dataset_id="test_dataset_formacao",
    table_parameters=gsheets_urls,
)

gsheets_one_minute_update_schedule = Schedule(clocks=untuple(gsheets_clocks))
