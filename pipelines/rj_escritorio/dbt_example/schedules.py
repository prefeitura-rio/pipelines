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
# Example DBT Schedule for Formação Infra
#
#####################################

example_dbt_tables = {
    "elementos": "elementos",
    "paises_americanos": "paises_americanos",
}

example_dbt_clocks = [
    IntervalClock(
        interval=timedelta(days=1),
        start_date=datetime(
            2022, 11, 17, 17, 30, tzinfo=pytz.timezone("America/Sao_Paulo")
        )
        + timedelta(minutes=3 * count),
        labels=[
            constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": "test_formacao",
            "table_id": table_id,
            "mode": "prod",
        },
    )
    for count, (_, table_id) in enumerate(example_dbt_tables.items())
]
example_dbt_tables_schedule = Schedule(
    clocks=untuple(example_dbt_clocks)
)
