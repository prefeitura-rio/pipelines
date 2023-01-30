# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
"""
Schedules for the database dump pipeline
"""

from datetime import timedelta

import pendulum
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants


project_ids = [
    "rj-escritorio",
    "rj-escritorio-dev",
    "rj-segovi",
    "rj-segovi-dev",
    "rj-seop",
    "rj-seop-dev",
    "rj-sme",
    "rj-sme-dev",
    "datario",
    "datario-dev",
    "rj-cor",
    "rj-cor-dev",
    "rj-smtr",
    "rj-smtr-dev",
    "rj-smtr-staging",
    "rj-seconserva",
    "rj-seconserva-dev",
    "rj-smfp",
    "rj-smfp-dev",
    "rj-siurb",
    "rj-iplanrio",
    "rj-iplanrio-dev",
    "rj-cetrio",
    "rj-cetrio-dev",
    "rj-comunicacao",
    "rj-comunicacao-dev",
    "rj-precipitacao",
    "rj-sms",
    "rj-sms-dev",
    "rj-rioaguas",
    "rj-rioaguas-dev",
    "rj-smi",
    "rj-smi-dev",
]


every_week = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(weeks=1),
            start_date=pendulum.datetime(2021, 1, 1, 4, 0, 0, tz="America/Sao_Paulo"),
            labels=[
                constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "project_ids": ",".join(project_ids),
            },
        )
    ]
)
