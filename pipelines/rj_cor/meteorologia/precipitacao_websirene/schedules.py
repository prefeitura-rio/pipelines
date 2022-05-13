# -*- coding: utf-8 -*-
"""
Schedules for precipitacao_alertario
Rodar a cada 30 ou 15 minutos
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

MINUTE_SCHEDULE = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=15),
            start_date=datetime(2021, 1, 1, 0, 3, 0),
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
        ),
    ]
)
