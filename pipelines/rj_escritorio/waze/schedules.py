# -*- coding: utf-8 -*-
"""
Schedules for emd
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

every_five_minutes = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1),
            # TODO change to RJ_DATARIO_AGENT_LABEL when it's ready
            labels=[
                constants.EMD_AGENT_LABEL.value,
            ],
        ),
    ]
)
