# -*- coding: utf-8 -*-
"""
Schedules for daily logs materialization.
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants

daily_at_4am = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),  # daily
            start_date=datetime(
                2021, 1, 1, 4, tzinfo=pytz.timezone("America/Sao_Paulo")  # 4am
            ),
            labels=[
                constants.RJ_DATARIO_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "secret_path": "birthday_webhook",
            },
        ),
    ]
)
