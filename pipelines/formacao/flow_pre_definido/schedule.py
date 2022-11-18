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
        "url": "https://docs.google.com/spreadsheets/d/1uF-Gt5AyZmxCQQEaebvWF4ddRHeVuL6ANuoaY_-uAXE\
            /edit#gid=0",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "sheet_1",
        "dump_mode": "overwrite",  # alterado
        "materialize_after_dump": True,  # alterado
        "materialization_mode": "dev",  # alterado
        "materialize_to_datario": True,  # alterado
        "dump_to_gcs": True,  # alterado
    },
}


gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(minutes=1),
    start_date=datetime(2022, 10, 21, 15, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
    dataset_id="test_dataset_formacao",
    table_parameters=gsheets_urls,
)

gsheets_one_minute_update_schedule = Schedule(clocks=untuple(gsheets_clocks))
