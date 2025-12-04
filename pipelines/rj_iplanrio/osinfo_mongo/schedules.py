# -*- coding: utf-8 -*-
"""
Schedules for OSINFO MongoDB pipelines
"""

from datetime import datetime, timedelta

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pytz import timezone

from pipelines.constants import constants

# Configuração de timezone
TZ = timezone("America/Sao_Paulo")

# Schedule para tabela files - execução diária às 6h
osinfo_mongo_files_daily_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2024, 1, 1, 6, 0, tzinfo=TZ),
            labels=[
                constants.RJ_IPLANRIO_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "osinfo_mongo",
                "table_id": "files",
                "materialize_after_dump": True,
                "materialization_mode": "dev",
                "materialize_to_datario": False,
            },
        )
    ]
)

# Schedule para tabela files_chunks - execução diária às 6h30
osinfo_mongo_files_chunks_daily_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2024, 1, 1, 6, 30, tzinfo=TZ),
            labels=[
                constants.RJ_IPLANRIO_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "osinfo_mongo",
                "table_id": "files_chunks",
                "materialize_after_dump": True,
                "materialization_mode": "dev",
                "materialize_to_datario": False,
            },
        )
    ]
)
