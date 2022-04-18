# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule
import pytz


from pipelines.constants import constants
from pipelines.utils.utils import untuple_clocks as untuple
from pipelines.utils.execute_dbt_model import generate_execute_dbt_model_schedules


diretorio_views = {
    "bairro": {"dataset_id": "diretorio", "mode": "dev"},
    "logradouro": {"dataset_id": "diretorio", "mode": "dev"},
}

diretorio_clocks = generate_execute_dbt_model_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2022, 3, 21, 3, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
    table_parameters=diretorio_views,
    runs_interval_minutes=5,
)

diretorio_weekly_update_schedule = Schedule(clocks=untuple(diretorio_clocks))
