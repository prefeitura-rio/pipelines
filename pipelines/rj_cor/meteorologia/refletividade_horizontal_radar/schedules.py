# -*- coding: utf-8 -*-
"""
Schedules for meteorologia/refletividade_horizontal_radar
Rodar a cada 5 minutos
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

minute_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=15),
            start_date=datetime(2021, 1, 1, 0, 5, 0),
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
        ),
    ]
)
