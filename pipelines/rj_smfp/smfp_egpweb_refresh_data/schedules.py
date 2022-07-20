# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from pipelines.constants import constants
from pipelines.utils.utils import untuple_clocks as untuple
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

#####################################
#
# SMFP Schedules
#
#####################################

smfp_egpweb_tables = {
    "chance": "chance",
    "comentario": "comentario",
    "indicador": "indicador",
    "meta": "meta",
    "nota_meta": "nota_meta",
}

smfp_egpweb_clocks = [
    IntervalClock(
        interval=timedelta(days=30),
        start_date=datetime(
            2022, 7, 20, 22, 30, tzinfo=pytz.timezone("America/Sao_Paulo")
        )
        + timedelta(minutes=15 * count),
        labels=[
            constants.RJ_SMFP_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": "planejamento_gestao_acordo_resultados",
            "table_id": table_id,
            "mode": "prod",
        },
    )
    for count, (_, table_id) in enumerate(smfp_egpweb_tables.items())
]
smfp_egpweb_monthly_update_schedule = Schedule(clocks=untuple(smfp_egpweb_clocks))
