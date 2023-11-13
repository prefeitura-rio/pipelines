# -*- coding: utf-8 -*-
"""
Schedules for the data catalog pipeline.
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants

update_flooding_data_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=3),
            start_date=datetime(2023, 1, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
            labels=[
                constants.RJ_ESCRITORIO_AGENT_LABEL.value,
            ],
            parameter_defaults={},  # TODO: Add parameters
        ),
    ]
)
