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


dados_mestresviews = {
    "bairro": {"dataset_id": "dados_mestres", "mode": "dev"},
    "logradouro": {"dataset_id": "dados_mestres", "mode": "dev"},
}

dados_mestresclocks = generate_execute_dbt_model_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2022, 3, 21, 3, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
    table_parameters=dados_mestresviews,
    runs_interval_minutes=5,
)

dados_mestresweekly_update_schedule = Schedule(clocks=untuple(dados_mestresclocks))
