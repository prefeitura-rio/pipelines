# -*- coding: utf-8 -*-
"""
Schedules
Rodar a cada 15 minutos
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

DAILY_SCHEDULE = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 0, 1, 0),
            labels=[
                constants.RJ_RIOAGUAS_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "materialization_mode": "prod",
                "dump_to_gcs": True,
        ),
    ]
)
