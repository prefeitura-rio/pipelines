# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import timedelta, datetime

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

sme_views = {
    "dependencia": "dependencia",
    "escola": "escola",
    "frequencia": "frequencia",
    "turma": "turma",
    "aluno": "aluno",
}

sme_clocks = [
    IntervalClock(
        interval=timedelta(days=30),
        start_date=datetime(
            2022, 2, 15, 12, 5, tzinfo=pytz.timezone("America/Sao_Paulo")
        )
        + timedelta(minutes=3 * count),
        labels=[
            constants.EMD_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": "educacao_basica",
            "table_id": table_id,
            "mode": "prod",
        },
    )
    for count, (_, table_id) in enumerate(sme_views.items())
]
sme_monthly_update_schedule = Schedule(clocks=untuple(sme_clocks))
