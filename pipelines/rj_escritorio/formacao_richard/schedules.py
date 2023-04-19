# -*- coding: utf-8 -*-
# flake8: noqa: E501
from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from pipelines.constants import constants
from pipelines.utils.dump_url.utils import generate_dump_url_schedules
from pipelines.utils.utils import untuple_clocks as untuple

gsheets_urls = {
    "gsheet": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1NLdmrcDDEyt5zFGGyrKulX9MlYgWw-ibU_FUTi5LMj8/edit?usp=sharing",
        "url_type": "google_sheet",
    }
}

gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2023, 4, 20, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
    dataset_id="",
    table_parameters=gsheets_urls,
)

gsheets_schedule = Schedule(clocks=untuple(gsheets_clocks))
