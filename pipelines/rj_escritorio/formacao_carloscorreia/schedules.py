# -*- coding: utf-8 -*-
# flake8: noqa: E501
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
# Disciplinas sem professor
#
#####################################

gsheets_urls = {
    "disciplinas_sem_professor": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qWaL24HyoCDcwqZorKVpeXPyJuhg-VxZeG3YbFtnCGU/view?usp=share_link",  # noqa
        "url_type": "google_sheet",
        "dataset_id": "teste_formacao",
    }
}

gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2021, 1, 1, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_ESCRITORIO_DEV_LABEL.value,
    ],
    dataset_id="",
    table_parameters=gsheets_urls,
)

gsheets_schedule = Schedule(clocks=untuple(gsheets_clocks))
