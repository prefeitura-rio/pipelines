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
# SMFP Dashboard de Metas Schedules
#
#####################################

smfp_dashboard_metas_tables = {
    "ar_detalhes": "ar_detalhes",
    "ar_valores": "ar_valores",
    "orgaos": "orgaos",
    "pe_detalhes": "pe_detalhes",
    "pe_numerico": "pe_numerico",
    "pe_porcentagem": "pe_porcentagem",
    "pe_ranking": "pe_ranking",
    "pe_textual": "pe_textual",
    "todos_detalhes": "todos_detalhes",
    "todos_numerico": "todos_numerico",
    "todos_percentual": "todos_percentual",
    "todos_ranking": "todos_ranking",
    "todos_textual": "todos_textual",
}

smfp_dashboard_metas_clocks = [
    IntervalClock(
        interval=timedelta(days=1),
        start_date=datetime(2021, 10, 24, 16, 0, tzinfo=pytz.timezone("America/Sao_Paulo"))
        + timedelta(minutes=3 * count),
        labels=[
            constants.RJ_SMFP_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": "planejamento_gestao_dashboard_metas",
            "table_id": table_id,
            "mode": "prod",
        },
    )
    for count, (_, table_id) in enumerate(smfp_dashboard_metas_tables.items())
]
smfp_dashboard_metas_daily_update_schedule = Schedule(clocks=untuple(smfp_dashboard_metas_clocks))
