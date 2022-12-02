# -*- coding: utf-8 -*-
"""
Schedules
Rodar a cada 5 minutos
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

MINUTE_SCHEDULE = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=10),
            start_date=datetime(2021, 1, 1, 0, 1, 0),
        ),
    ]
)
