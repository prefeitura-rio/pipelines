# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Schedules for rj_escritorio.rain_dashboard.
"""

from datetime import timedelta

import pendulum
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants
from pipelines.rj_escritorio.rain_dashboard.constants import (
    constants as rain_dashboard_constants,
)

every_fifteen_minutes = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=15),
            start_date=pendulum.datetime(2021, 1, 1, 0, 5, 0, tz="America/Sao_Paulo"),
            labels=[
                constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_SCHEDULE_PARAMETERS.value,
        )
    ]
)
