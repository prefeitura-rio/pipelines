# -*- coding: utf-8 -*-
"""
Schedules for meteorologia_inmet
Rodar a cada 1 hora
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

hour_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 6, 0),#9, 14, 19, 24, 29, 34, 39, 44, 49, 54, 59
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
        ),
    ]
)
