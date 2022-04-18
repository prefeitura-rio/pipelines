# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule
import pytz


from pipelines.constants import constants
from pipelines.utils.utils import untuple_clocks as untuple
from pipelines.utils.execute_dbt_model.utils import generate_execute_dbt_model_schedules


_1746_views = {
    "chamado": {"dataset_id": "administracao_servicos_publicos_1746", "mode": "prod"},
}

_1746_clocks = generate_execute_dbt_model_schedules(
    interval=timedelta(days=30),
    start_date=datetime(2022, 3, 21, 4, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SEGOVI_AGENT_LABEL.value,
    ],
    table_parameters=_1746_views,
    runs_interval_minutes=5,
)

_1746_monthly_update_schedule = Schedule(clocks=untuple(_1746_clocks))
