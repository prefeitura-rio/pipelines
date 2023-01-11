# -*- coding: utf-8 -*-
"""
Schedules for daily logs materialization.
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.execute_dbt_model.utils import generate_execute_dbt_model_schedules
from pipelines.utils.utils import untuple_clocks as untuple

materialize_smi_flow_schedule_parameters = {
    "obra": {
        "dataset_id": "infraestrutura_siscob_obras_dashboard",
        "mode": "prod",
        "dbt_alias": True,
    },
    "localizacao": {
        "dataset_id": "infraestrutura_siscob_obras_dashboard",
        "mode": "prod",
        "dbt_alias": True,
    },
    "medicao": {
        "dataset_id": "infraestrutura_siscob_obras_dashboard",
        "mode": "prod",
        "dbt_alias": True,
    },
    "programa_fonte": {
        "dataset_id": "infraestrutura_siscob_obras_dashboard",
        "mode": "prod",
        "dbt_alias": True,
    },
}

materialize_smi_flow_schedule_clocks = generate_execute_dbt_model_schedules(
    interval=timedelta(days=30),
    start_date=datetime(2022, 12, 19, 3, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMI_AGENT_LABEL.value,
    ],
    table_parameters=materialize_smi_flow_schedule_parameters,
    runs_interval_minutes=15,
)

materialize_smi_flow_schedule = Schedule(
    clocks=untuple(materialize_smi_flow_schedule_clocks)
)
