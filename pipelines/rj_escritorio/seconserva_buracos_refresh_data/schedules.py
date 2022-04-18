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
# SME Schedules
#
#####################################

seconserva_buracos_tables = {
    "chamados_estruturados": "chamados_estruturados",
    "chamados_priorizados": "chamados_priorizados",
}

seconserva_buracos_clocks = [
    IntervalClock(
        interval=timedelta(days=1),
        start_date=datetime(2022, 4, 1, 2, 0, tzinfo=pytz.timezone("America/Sao_Paulo"))
        + timedelta(minutes=15 * count),
        labels=[
            constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": "seconserva_buracos",
            "table_id": table_id,
            "mode": "dev",
        },
    )
    for count, (_, table_id) in enumerate(seconserva_buracos_tables.items())
]
seconserva_buracos_daily_update_schedule = Schedule(
    clocks=untuple(seconserva_buracos_clocks)
)
