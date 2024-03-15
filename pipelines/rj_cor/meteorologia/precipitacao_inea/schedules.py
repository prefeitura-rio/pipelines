# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for precipitacao_inea
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
            start_date=datetime(2023, 1, 1, 0, 1, 0),
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
                "dataset_id_pluviometric": "clima_pluviometro",
                "table_id_pluviometric": "taxa_precipitacao_inea",
                "dataset_id_fluviometric": "clima_fluviometro",
                "table_id_fluviometric": "lamina_agua_inea",
            },
        ),
    ]
)
