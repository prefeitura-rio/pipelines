# -*- coding: utf-8 -*-
"""
Schedules to run all satelite products
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

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
                "table_id": "taxa_precipitacao_goes_16",
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
dsi = Schedule(
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
                "table_id": "indice_estabilidade_derivado_goes_16",
                "product": "DSIF",
            },
        )
    ]
)
lst = Schedule(
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
                "table_id": "temperatura_superficie_terra_goes_16",
                "product": "LSTF",
            },
        )
    ]
)
sst = Schedule(
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
                "table_id": "temperatura_superficie_oceano_goes_16",
                "product": "SSTF",
            },
        )
    ]
)
aod = Schedule(
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
                "table_id": "profundidade_optica_aerossol_goes_16",
                "product": "AODF",
            },
        )
    ]
)
