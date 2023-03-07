# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
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
# Setur Schedules
#
#####################################

gsheets_urls = {
    "museu_do_amanha": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1wio45Se6HXo1nFeyY2GXxa-fduJlnICVW1p_Rg4UbjI\
            /edit#gid=722381052",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Museu do Amanhã",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "cristo_redentor": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1wio45Se6HXo1nFeyY2GXxa-fduJlnICVW1p_Rg4UbjI\
            /edit#gid=722381052",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Cristo Redentor (Paineiras)",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "bio_parque": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1wio45Se6HXo1nFeyY2GXxa-fduJlnICVW1p_Rg4UbjI\
            /edit#gid=722381052",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Bio Parque",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "aquario": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1wio45Se6HXo1nFeyY2GXxa-fduJlnICVW1p_Rg4UbjI\
            /edit#gid=722381052",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "AquaRio",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "ccbb": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1wio45Se6HXo1nFeyY2GXxa-fduJlnICVW1p_Rg4UbjI\
            /edit#gid=722381052",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "CCBB",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "iss": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1wio45Se6HXo1nFeyY2GXxa-fduJlnICVW1p_Rg4UbjI\
            /edit#gid=722381052",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "ISS",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "galeao": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1wio45Se6HXo1nFeyY2GXxa-fduJlnICVW1p_Rg4UbjI\
            /edit#gid=722381052",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Galeão",
        "gsheets_sheet_range": "A5:D1000",
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "caged": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1wio45Se6HXo1nFeyY2GXxa-fduJlnICVW1p_Rg4UbjI\
            /edit#gid=722381052",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "CAGED",
        "gsheets_sheet_range": "A5:F1000",
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
}


gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(days=1),
    runs_interval_minutes=1,
    start_date=datetime(
        2021, 12, 21, 14, 20, tzinfo=pytz.timezone("America/Sao_Paulo")
    ),
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
    table_parameters=gsheets_urls,
    dataset_id="rj_setur_turismo",
)

gsheets_daily_update_schedule = Schedule(clocks=untuple(gsheets_clocks))
