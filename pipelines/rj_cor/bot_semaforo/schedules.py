"""
Schedules for cor
"""

from datetime import timedelta, datetime, time

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants


def custom_time(
    hour: int = 0, minute: int = 0, second: int = 0, timezone: str = "America/Sao_Paulo"
):
    """
    Returns a time object with the given hour, minute and second.
    """
    return time(hour, minute, second, tzinfo=pytz.timezone(timezone))


bot_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(hours=1),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "secret_path": "cet-bot",
            },
        ),
    ],
)
