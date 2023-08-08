# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
import pytz

from pipelines.constants import constants
from pipelines.utils.execute_dbt_model.utils import generate_execute_dbt_model_schedules
from pipelines.utils.utils import untuple_clocks as untuple
from pipelines.rj_smtr.tasks import get_previous_date

dbt_model_parameters = {
    "date_range_end": get_previous_date.run(1),
    "date_range_start": None,
}

smtr_materialize_to_datario_parameters = {
    "gps_brt": {
        "dataset_id": "transporte_rodoviario_municipal",
        "table_id": "gps_brt",
        "mode": "prod",
        "materialize_to_datario": True,
        "dbt_model_parameters": dbt_model_parameters,
    },
    "gps_onibus": {
        "dataset_id": "transporte_rodoviario_municipal",
        "table_id": "gps_onibus",
        "mode": "prod",
        "materialize_to_datario": True,
        "dbt_model_parameters": dbt_model_parameters,
    },
    "viagem_onibus": {
        "dataset_id": "transporte_rodoviario_municipal",
        "table_id": "viagem_onibus",
        "mode": "prod",
        "materialize_to_datario": True,
        "dbt_model_parameters": dbt_model_parameters,
    },
    "viagem_planejada_onibus": {
        "dataset_id": "transporte_rodoviario_municipal",
        "table_id": "viagem_planejada_onibus",
        "mode": "prod",
        "materialize_to_datario": True,
        "dbt_model_parameters": dbt_model_parameters,
    },
}

smtr_materialize_to_datario_clocks = generate_execute_dbt_model_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2020, 1, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_DATARIO_AGENT_LABEL.value,
    ],
    table_parameters=smtr_materialize_to_datario_parameters,
    runs_interval_minutes=15,
)

smtr_materialize_to_datario_daily_schedule = Schedule(
    clocks=untuple(smtr_materialize_to_datario_clocks)
)
