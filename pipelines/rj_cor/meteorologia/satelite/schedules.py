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
            interval=timedelta(hours=1),
            start_date=datetime(2021, 1, 1, 0, 15, 0),
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
        ),
    ]
)
