# -*- coding: utf-8 -*-
"""
Schedules for meteorologia_redemet
Rodar a cada 1 hora
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

hour_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(2023, 1, 1, 0, 12, 0),
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
            parameter_defaults={
                # "trigger_rain_dashboard_update": True,
                "materialize_after_dump": True,
                "mode": "prod",
                "materialize_to_datario": True,
                "dump_to_gcs": False,
                "dump_mode": "append",
                "dataset_id": "clima_estacao_meteorologica",
                "table_id": "meteorologia_redemet",
            },
        ),
    ]
)

month_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(months=1),
            start_date=datetime(2023, 1, 1, 0, 12, 0),
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "materialize_after_dump": True,
                "mode": "prod",
                "materialize_to_datario": True,
                "dump_to_gcs": False,
                # "dump_mode": "overwrite",
                # "dataset_id": "clima_estacao_meteorologica",
                # "table_id": "estacoes_redemet",
            },
        ),
    ]
)
