"""
Schedules for the database dump pipeline
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants
from pipelines.utils.utils import untuple_clocks as untuple


def get_clock(dataset_id, table_id, count):
    """
    Returns a clock for the given dataset and table with an offset of
    `count` * 3 minutes.
    """
    return IntervalClock(
        interval=timedelta(days=30),
        start_date=datetime(
            2022, 2, 15, 12, 5, tzinfo=pytz.timezone("America/Sao_Paulo")
        )
        + timedelta(minutes=3 * count),
        labels=[
            constants.EMD_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "dataset_id": dataset_id,
            "table_id": table_id,
            "mode": "prod",
        },
    )


#####################################
#
# 1746 Schedules
#
#####################################

_1746_views = {
    "chamado": "chamado",
}

_1746_clocks = [
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
            "dataset_id": "administracao_servicos_publicos_1746",
            "table_id": table_id,
            "mode": "prod",
        },
    )
    for count, (_, table_id) in enumerate(_1746_views.items())
]
_1746_monthly_update_schedule = Schedule(clocks=untuple(_1746_clocks))
