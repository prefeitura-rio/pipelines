# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for precipitacao_alertario
Rodar a cada 1 minuto
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

minute_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 1, 0),
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
            parameter_defaults={
                # "trigger_rain_dashboard_update": True,
                "materialize_after_dump": True,
                "mode": "prod",
                "materialize_to_datario": True,
                "dump_to_gcs": False,
            },
        ),
    ]
)

day_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 5, 0, 30),
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
            parameter_defaults={
                # "trigger_rain_dashboard_update": True,
                "materialize_after_dump": True,
                "mode": "prod",
                "materialize_to_datario": True,
                "dump_to_gcs": False,
            },
        ),
    ]
)
