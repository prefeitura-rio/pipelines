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
# SMI Dashboard de Obras Schedules
#
#####################################

smi_dashboard_obras_tables = {
    "localizacao": "localizacao",
    "medicao": "medicao",
    "obra": "obra",
    "programa_fonte": "programa_fonte",
}

smi_dashboard_obras_clocks = [
    IntervalClock(
        interval=timedelta(days=1),
        start_date=datetime(
            2023, 4, 20, 18, 0, tzinfo=pytz.timezone("America/Sao_Paulo")
        )
        + timedelta(minutes=2 * count),
        labels=[
            constants.RJ_SMI_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": "infraestrutura_siscob_obras_dashboard",
            "table_id": table_id,
            "mode": "prod",
        },
    )
    for count, (_, table_id) in enumerate(smi_dashboard_obras_tables.items())
]
smi_dashboard_obras_daily_update_schedule = Schedule(
    clocks=untuple(smi_dashboard_obras_clocks)
)
