# -*- coding: utf-8 -*-
"""
Schedules for the SEGOVI SICOP pipeline
"""
from datetime import datetime, timedelta

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
                "dump_mode": "overwrite", # alterado
                "materialize_after_dump": True, # alterado
                "materialization_mode": "dev", # alterado
                "materialize_to_datario": True, # alterado
                "dump_to_gcs": True, # alterado
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
                "dump_mode": "overwrite", # alterado
                "materialize_after_dump": True, # alterado
                "materialization_mode": "dev", # alterado
                "materialize_to_datario": True, # alterado
                "dump_to_gcs": True, # alterado
            },
        ),
    ]
)
