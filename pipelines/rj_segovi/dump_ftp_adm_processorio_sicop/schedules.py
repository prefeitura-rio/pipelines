# -*- coding: utf-8 -*-
"""
Schedules for the SEGOVI SICOP pipeline
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

every_week_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=7),
            start_date=datetime(2022, 1, 1, 12, 40, 0),
            labels=[
                constants.RJ_SEGOVI_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "pattern": "processo",
                "dataset_id": "adm_processorio_sicop",
                "table_id": "processo",
            },
        ),
        IntervalClock(
            interval=timedelta(days=7),
            start_date=datetime(2022, 1, 1, 12, 50, 0),
            labels=[
                constants.RJ_SEGOVI_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "pattern": "expediente",
                "dataset_id": "adm_processorio_sicop",
                "table_id": "expediente",
            },
        ),
    ]
)
