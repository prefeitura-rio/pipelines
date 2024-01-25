# -*- coding: utf-8 -*-
"""
Schedules for the COR pipeline
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

every_hour = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=15),
            start_date=datetime(2022, 7, 19, 12, 50, 0),
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "materialize_to_datario": True,
                "dump_to_gcs": True,
                "trigger_rain_dashboard_update": True,
                "redis_mode": "prod",
            },
        ),
    ]
)

every_month = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(2022, 1, 1, 12, 40, 0),
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "materialize_to_datario": True,
                "dump_to_gcs": True,
            },
        ),
    ]
)
