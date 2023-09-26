# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import datetime, timedelta

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants
from pipelines.utils.utils import untuple_clocks as untuple

#####################################
#
# Comunicação Executiva Schedules
#
#####################################

# comunicacao_executiva_tables = {
#     "data_atualizacao": "data_atualizacao",
#     "performance_subprefeituras_1746": "performance_subprefeituras_1746",
#     "performance_geral_1746": "performance_geral_1746",
# }

diariamente = {
    "data_atualizacao": "data_atualizacao",
}

mensalmente_dia_1 = {
    "performance_subprefeituras_1746": "performance_subprefeituras_1746",
    "performance_geral_1746": "performance_geral_1746",
}

mensalmente_dia_20 = {
    "pontos_turisticos": "pontos_turisticos",
    "aeroportos": "aeroportos",
}

diario_clocks = [
    IntervalClock(
        interval=timedelta(days=1),
        start_date=datetime(
            2023, 5, 16, 17, 0, tzinfo=pytz.timezone("America/Sao_Paulo")
        )
        + timedelta(minutes=1 * count),
        labels=[
            constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": "comunicacao_executiva",
            "table_id": table_id,
            "mode": "dev",
        },
    )
    for count, (_, table_id) in enumerate(diariamente.items())
]

mensal_1_clocks = [
    IntervalClock(
        interval=timedelta(days=30),
        start_date=datetime(
            2023, 6, 1, 23, 50, tzinfo=pytz.timezone("America/Sao_Paulo")
        )
        + timedelta(minutes=1 * count),
        labels=[
            constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": "comunicacao_executiva",
            "table_id": table_id,
            "mode": "dev",
        },
    )
    for count, (_, table_id) in enumerate(mensalmente_dia_1.items())
]

mensal_20_clocks = [
    IntervalClock(
        interval=timedelta(days=30),
        start_date=datetime(
            2023, 6, 20, 23, 0, tzinfo=pytz.timezone("America/Sao_Paulo")
        )
        + timedelta(minutes=1 * count),
        labels=[
            constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": "comunicacao_executiva",
            "table_id": table_id,
            "mode": "dev",
        },
    )
    for count, (_, table_id) in enumerate(mensalmente_dia_20.items())
]

comunicacao_executiva_clocks = diario_clocks + mensal_1_clocks + mensal_20_clocks

comunicacao_executiva_daily_update_schedule = Schedule(
    clocks=untuple(comunicacao_executiva_clocks)
)
