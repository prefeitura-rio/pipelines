# -*- coding: utf-8 -*-

"""
Schedule for dump_url_remenber
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.dump_url.utils import generate_dump_url_schedules
from pipelines.utils.utils import untuple_clocks as untuple

gsheets_url = {
    "db_documentos": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/1qV80h5zOLshGdO48xUmJMnibBWtC2Cyr/view?usp=sharing",
        "url_type": "google_drive",
        "materialization_mode": "prod",
        "materialize_after_dump": True,
        "materialize_to_datario": False,
        "dataset_id": "db_remember_teste",
    }
}

gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(days=365),
    start_date=datetime(2023, 2, 7, 0, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SEGOVI_AGENT_LABEL.value,
    ],
    table_parameters=gsheets_url,
    dataset_id="db_remember_teste",
)

gsheets_daily_update_schedule = Schedule(clocks=untuple(gsheets_clocks))
