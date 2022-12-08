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
    "assunto_pgm": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/1ZF71cmU4T03iALerMuN2cOFuFYB6h62s/view?usp=share_link",  # noqa
        "url_type": "google_drive",
        "materialization_mode": "prod",
        "materialize_after_dump": True,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dataset_id": "adm_processorio_sicop",
    },
    "assunto": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/1JCZhr8_Zww3jzF6xSzWG9WZcdLot0ZrS/view?usp=share_link",  # noqa
        "url_type": "google_drive",
        "materialization_mode": "prod",
        "materialize_after_dump": True,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dataset_id": "adm_processorio_sicop",
    },
    "sici": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/1OMh3gClqA1KkIIBznAry29NR-UD__Z-b/view?usp=share_link",  # noqa
        "url_type": "google_drive",
        "materialization_mode": "prod",
        "materialize_after_dump": True,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dataset_id": "adm_processorio_sicop",
    },
    "codigo_sicop": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/1aQl9NNVeUCrAvXPP4e6zaFEp-YUDCPdX/view?usp=share_link",  # noqa
        "url_type": "google_drive",
        "materialization_mode": "prod",
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "dataset_id": "adm_processorio_sicop",
    },
}


gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(days=360),
    start_date=datetime(2022, 11, 29, 14, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SEGOVI_AGENT_LABEL.value,
    ],
    table_parameters=gsheets_urls,
    dataset_id="adm_processorio_sicop",
)

gsheets_daily_update_schedule = Schedule(clocks=untuple(gsheets_clocks))
