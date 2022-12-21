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
    "metrica_1": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1VUTYACCrvsFVnTHgICsgUh712gpV9x1_/edit#gid=1536258547",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_order": 1,
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "metrica_2": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1VUTYACCrvsFVnTHgICsgUh712gpV9x1_/edit#gid=1536258547",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_order": 2,
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "metrica_3": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1VUTYACCrvsFVnTHgICsgUh712gpV9x1_/edit#gid=1536258547",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_order": 3,
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "metrica_4": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1VUTYACCrvsFVnTHgICsgUh712gpV9x1_/edit#gid=1536258547",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_order": 4,
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "metrica_5": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1VUTYACCrvsFVnTHgICsgUh712gpV9x1_/edit#gid=1536258547",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_order": 5,
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "metrica_6": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1VUTYACCrvsFVnTHgICsgUh712gpV9x1_/edit#gid=1536258547",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_order": 6,
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "metrica_7": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1VUTYACCrvsFVnTHgICsgUh712gpV9x1_/edit#gid=1536258547",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_order": 7,
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "metrica_8": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1VUTYACCrvsFVnTHgICsgUh712gpV9x1_/edit#gid=1536258547",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_order": 8,
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "metrica_9": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1VUTYACCrvsFVnTHgICsgUh712gpV9x1_/edit#gid=1536258547",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_order": 9,
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "metrica_10": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1VUTYACCrvsFVnTHgICsgUh712gpV9x1_/edit#gid=1536258547",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_order": 10,
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "metrica_11": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1VUTYACCrvsFVnTHgICsgUh712gpV9x1_/edit#gid=1536258547",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_order": 11,
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "metrica_12": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1VUTYACCrvsFVnTHgICsgUh712gpV9x1_/edit#gid=1536258547",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_order": 12,
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "metrica_13": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1VUTYACCrvsFVnTHgICsgUh712gpV9x1_/edit#gid=1536258547",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_order": 13,
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "metrica_14": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1VUTYACCrvsFVnTHgICsgUh712gpV9x1_/edit#gid=1536258547",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_order": 14,
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
    "metrica_15": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1VUTYACCrvsFVnTHgICsgUh712gpV9x1_/edit#gid=1536258547",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_order": 15,
        "materialize_after_dump": True,
        "dataset_id": "rj_setur_turismo",
    },
}


gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2021, 12, 21, 16, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
    table_parameters=gsheets_urls,
    dataset_id="rj_setur_turismo",
)

gsheets_daily_update_schedule = Schedule(clocks=untuple(gsheets_clocks))
