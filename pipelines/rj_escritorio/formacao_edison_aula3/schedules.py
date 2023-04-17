# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Schedules Carga de Google Sheet para DataLake
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from pipelines.constants import constants
from pipelines.utils.dump_url.utils import generate_dump_url_schedules
from pipelines.utils.utils import untuple_clocks as untuple

#####################################
#
# GoogleSheet Basica para Datalake
#
#####################################

gsheets_urls = {
    "sheet": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1cr6EYi3-IKSnCBbrBVsI7o5CTFmUV7h6uQ2kMGAysyg/edit#gid=0",  # noqa
        "url_type": "google_sheet",
    }
}

gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(days=3),
    start_date=datetime(2023, 3, 14, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
    dataset_id="",
    table_parameters=gsheets_urls,
)

gsheets_schedule = Schedule(clocks=untuple(gsheets_clocks))
