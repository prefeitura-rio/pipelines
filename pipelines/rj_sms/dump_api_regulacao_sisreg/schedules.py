# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import timedelta
import pendulum
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

every_day_at_six_am = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=pendulum.datetime(2023, 1, 1, 6, 0, 0, tz="America/Sao_Paulo"),
            labels=[
                constants.RJ_SMS_DEV_AGENT_LABEL.value,
            ],
        )
    ]
)
