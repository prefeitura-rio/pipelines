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

diariamente = [
    {
        "dataset_id": "povo_comunidades_tradicionais",
        "table_id": "indepit",
        "mode": "prod",
    },
    {
        "dataset_id": "povo_comunidades_tradicionais",
        "table_id": "visita_valongo",
        "mode": "prod",
    },
]


diario_clocks = [
    IntervalClock(
        interval=timedelta(days=1),
        start_date=datetime(2023, 1, 1, 2, 0, tzinfo=pytz.timezone("America/Sao_Paulo"))
        + timedelta(minutes=2 * count),
        labels=[
            constants.RJ_SMAC_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": table["dataset_id"],
            "table_id": table["table_id"],
            "mode": table["mode"],
        },
    )
    for count, table in enumerate(diariamente)
]

materialize_povo_comunidades_tradicionais_clocks = diario_clocks

materialize_povo_comunidades_tradicionais_schedule = Schedule(
    clocks=untuple(materialize_povo_comunidades_tradicionais_clocks)
)
