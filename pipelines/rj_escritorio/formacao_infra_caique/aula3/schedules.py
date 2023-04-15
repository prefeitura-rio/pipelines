# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Schedule para a formação em infraestrutura - aula 3
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from pipelines.constants import constants
from pipelines.utils.dump_url.utils import generate_dump_url_schedules
from pipelines.utils.utils import untuple_clocks as untuple

gsheets_urls = {
    "sheet": {  # nome da tabela
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1q-upTF4Dz7yTCVYonE9_8jt1wDHz3qYe8bLo3IN2tsY/edit?usp=sharing",  # noqa
        "url_type": "google_sheet",
        "dataset_id": "formacao-caique-aula3",
    }
}

gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2021, 1, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
    dataset_id="",
    table_parameters=gsheets_urls,
)

gsheets_schedule = Schedule(clocks=untuple(gsheets_clocks))
