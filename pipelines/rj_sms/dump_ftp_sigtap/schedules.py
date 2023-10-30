# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the cnes dump pipeline
"""
from datetime import timedelta
import pendulum
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

every_sunday_at_six_am = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=7),
            start_date=pendulum.datetime(2023, 10, 8, 6, 0, 0, tz="America/Sao_Paulo"),
            labels=[
                constants.RJ_SMS_DEV_AGENT_LABEL.value,
            ],
        )
    ]
)
