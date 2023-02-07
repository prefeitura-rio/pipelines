# -*- coding: utf-8 -*-
"""
Schedules de nível de lâmina de água em via.
Rodar a cada 10 minutos
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

MINUTE_SCHEDULE = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=10),
            start_date=datetime(2021, 1, 1, 0, 1, 0),
            labels=[
                constants.RJ_RIOAGUAS_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "materialize_after_dump": True,
                "mode": "prod",
                "materialize_to_datario": False,
                "dump_to_gcs": False,
            },
        ),
    ]
)
