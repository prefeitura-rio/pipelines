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
# SETUR Seeketing Schedules
#
#####################################

setur_seeketing_tables = {
    "aeroportos": "aeroportos",
    "pontos_turisticos": "pontos_turisticos",
    "caged": "caged",
    "empregos": "empregos",
    "iss_turistico": "iss_turistico",
    "rede_hoteleira_ocupacao_eventos": "rede_hoteleira_ocupacao_eventos",
    "rede_hoteleira_ocupacao_geral": "rede_hoteleira_ocupacao_geral",
    "rodoviarias": "rodoviarias",
}

setur_seeketing_clocks = [
    IntervalClock(
        interval=timedelta(days=1),
        start_date=datetime(
            2023, 3, 28, 17, 30, tzinfo=pytz.timezone("America/Sao_Paulo")
        )
        + timedelta(minutes=1 * count),
        labels=[
            constants.RJ_SETUR_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": "turismo_fluxo_visitantes",
            "table_id": table_id,
            "mode": "prod",
        },
    )
    for count, (_, table_id) in enumerate(setur_seeketing_tables.items())
]
setur_seeketing_daily_update_schedule = Schedule(clocks=untuple(setur_seeketing_clocks))
