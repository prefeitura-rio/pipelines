# -*- coding: utf-8 -*-
"""
Schedules for meteorologia_inmet
Rodar a cada 1 hora
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

hour_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 4, 0),
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
        ),
    ]
)
rrqpe = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 4, 0),
            labels=[constants.RJ_COR_AGENT_LABEL.value],
            parameter_defaults={
                "materialize_after_dump": False,
                "materialize_to_datario": False,
                "mode": "prod",
                "mode_redis": "prod",
                "dataset_id": "clima_satelite",
                "table_id": "taxa_precipitacao_goes_16_temp",
                "product": "RRQPEF",
            },
        )
    ]
)
tpw = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 4, 0),
            labels=[constants.RJ_COR_AGENT_LABEL.value],
            parameter_defaults={
                "materialize_after_dump": False,
                "materialize_to_datario": False,
                "mode": "prod",
                "mode_redis": "prod",
                "dataset_id": "clima_satelite",
                "table_id": "quantidade_agua_precipitavel_goes_16",
                "product": "TPWF",
            },
        )
    ]
)
cmip = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 4, 0),
            labels=[constants.RJ_COR_AGENT_LABEL.value],
            parameter_defaults={
                "materialize_after_dump": False,
                "materialize_to_datario": False,
                "mode": "prod",
                "mode_redis": "prod",
                "dataset_id": "clima_satelite",
                "table_id": "infravermelho_longo_banda_13_goes_16",
                "product": "CMIPF",
                "band": "13",
            },
        )
    ]
)
mcmip = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 4, 0),
            labels=[constants.RJ_COR_AGENT_LABEL.value],
            parameter_defaults={
                "materialize_after_dump": False,
                "materialize_to_datario": False,
                "mode": "prod",
                "mode_redis": "prod",
                "dataset_id": "clima_satelite",
                "table_id": "imagem_nuvem_umidade_goes_16",
                "product": "MCMIPF",
            },
        )
    ]
)
