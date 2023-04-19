# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Schedule - Carga de Google Sheet para DataLake
"""
from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from pipelines.constants import constants
from pipelines.utils.dump_url.utils import generate_dump_url_schedules
from pipelines.utils.utils import untuple_clocks as untuple

#####################################
#
# Fonte GoogleSheet Basica para Datalake
#
#####################################

gsheets_urls = {
    "sheet": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1nuj1wOX_CeNYyyAdxAZzkVZ2RblzNQIELOFEdHid2A0/edit?usp=sharing",
        "url_type": "google_sheet",
        "materialize_after_dump": True,
        "dataset_id": "formacao_edison",
        "table_id": "aula3",
        "gsheets_sheet_order": 1,
    }
}

gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(days=3),
    start_date=datetime(2023, 3, 14, 20, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
    dataset_id="formacao_edison",
    table_parameters=gsheets_urls,
    batch_data_type="csv",
    runs_interval_minutes=360,
)

gsheets_schedule = Schedule(clocks=untuple(gsheets_clocks))
