# -*- coding: utf-8 -*-
"""
Schedules for the daily birthday flow.
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants

every_5_minutes = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
            labels=[
                constants.RJ_SMS_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "name": "scheduled flow",
            },
        ),
    ]
)
