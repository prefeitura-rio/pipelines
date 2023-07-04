# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for precipitacao_cemaden
Rodar a cada 1 minuto
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

minute_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=1),
            start_date=datetime(2023, 1, 1, 0, 0, 30),
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
            parameter_defaults={
                # "trigger_rain_dashboard_update": True,
                # "materialize_after_dump": True,
                "materialize_after_dump": False,
                # "mode": "prod",
                "materialize_to_datario": False,
                "dump_to_gcs": False,
            },
        ),
    ]
)
