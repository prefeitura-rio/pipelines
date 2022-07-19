# -*- coding: utf-8 -*-
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

every_day = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 7, 19, 12, 50, 0),
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "materialize_after_dump": False,
                "materialization_mode": "dev",
                "materialize_to_datario": False,
                "dump_to_gcs": False,
            },
        ),
    ]
)
