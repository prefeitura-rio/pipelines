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
        "url": "https://drive.google.com/file/d/1c5bSsVgmjb1m3z39cRlyKX9m3vhZHj5S/view?usp=share_link",
        "url_type": "google_drive",
        "materialize_after_dump": True,
        "dataset_id": "educacao_basica_alocacao",
    }
}

gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(days=365),
    start_date=datetime(2022, 11, 4, 20, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SME_AGENT_LABEL.value,
    ],
    dataset_id="",
    table_parameters=gsheets_urls,
)

gsheets_year_update_schedule = Schedule(clocks=untuple(gsheets_clocks))
