from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants

daily_at_9am = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2021, 1, 1, 9, tzinfo=pytz.timezone("America/Sao_Paulo")),
            labels=[
                constants.EMD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "secret_path": "birthday_webhook",
            },
        ),
    ]
)
