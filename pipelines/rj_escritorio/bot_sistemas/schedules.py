# -*- coding: utf-8 -*-
"""
Schedules for the systems telegram bot.
"""

from datetime import datetime, timedelta

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants

PARAMETER_DEFAULTS = {
    "secret_path": "systems-bot",
    "sheet_id": "1xuQOo0Z9uwPVwVvmNhwpgFndKGD9c2tttX2nPR5FOVM",
    "sheet_name": "sistemas_prioritarios_2",
}

systems_telegram_bot_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2021, 1, 1, 6, tzinfo=pytz.timezone("America/Sao_Paulo")
            ),  # 6 AM
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
            parameter_defaults=PARAMETER_DEFAULTS,
        ),
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2021, 1, 1, 12, tzinfo=pytz.timezone("America/Sao_Paulo")
            ),  # 12 PM
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
            parameter_defaults=PARAMETER_DEFAULTS,
        ),
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2021, 1, 1, 20, tzinfo=pytz.timezone("America/Sao_Paulo")
            ),  # 8 PM
            labels=[
                constants.RJ_COR_AGENT_LABEL.value,
            ],
            parameter_defaults=PARAMETER_DEFAULTS,
        ),
    ],
)
