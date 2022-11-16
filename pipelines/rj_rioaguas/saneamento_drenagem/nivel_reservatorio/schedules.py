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
    "test_table": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/u/1/d/e/2PACX-1vQd3-V6K_hOcrVySYJKk0tevS9TCI0M\
        pwQ5W7IY-_fIUUR4uZ0JVttqmaHeA9Pm-BJsAXUmjTvLZaDt/pubhtml?widget=true&headers=false#gid=1343\
        658906",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Reservatórios",
    },
}


gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(minutes=1),
    start_date=datetime(2022, 11, 17, 12, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
    dataset_id="saneamento_drenagem",
    table_parameters=gsheets_urls,
)

gsheets_five_minute_update_schedule = Schedule(clocks=untuple(gsheets_clocks))
