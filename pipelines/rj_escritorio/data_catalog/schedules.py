# -*- coding: utf-8 -*-
"""
Schedules for the data catalog pipeline.
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants

project_ids = [
    "datario",
    # "datario-dev",
    "rj-cetrio",
    # "rj-cetrio-dev",
    # "rj-chatbot",
    # "rj-chatbot-dev",
    "rj-comunicacao",
    # "rj-comunicacao-dev",
    "rj-cor",
    # "rj-cor-dev",
    "rj-escritorio",
    # "rj-escritorio-dev",
    "rj-iplanrio",
    # "rj-iplanrio-dev",
    "rj-pgm",
    "rj-precipitacao",
    "rj-rioaguas",
    # "rj-rioaguas-dev",
    "rj-seconserva",
    # "rj-seconserva-dev",
    "rj-segovi",
    # "rj-segovi-dev",
    "rj-seop",
    # "rj-seop-dev",
    "rj-setur",
    # "rj-setur-dev",
    "rj-siurb",
    # "rj-smac-dev",
    "rj-smas",
    "rj-sme",
    # "rj-sme-dev",
    "rj-smfp",
    # "rj-smfp-dev",
    "rj-smfp-egp",
    "rj-smi",
    # "rj-smi-dev",
    "rj-sms",
    # "rj-sms-dev",
    "rj-smtr",
    # "rj-smtr-staging",
    # "rj-smtr-dev",
]

update_data_catalog_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=7),
            start_date=datetime(
                2023, 3, 12, 23, 50, tzinfo=pytz.timezone("America/Sao_Paulo")
            ),
            labels=[
                constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "project_ids": ",".join(project_ids),
                "spreadsheet_url": "https://docs.google.com/spreadsheets/d/1U3KJ3xZSS8J1AGzVyuGigplNzRLuRdSHzs6IGPw-anY/edit#gid=0",  # noqa
                "sheet_name": "catalogo",
                "bq_client_mode": "prod",
            },
        ),
    ]
)
