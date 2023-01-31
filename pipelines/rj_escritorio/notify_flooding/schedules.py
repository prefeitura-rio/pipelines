# -*- coding: utf-8 -*-
"""
Schedules for the notify_flooding pipeline.
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants

test_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
            labels=[
                constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "email_configuration_secret_path": "smtp-escritoriodedados-gmail",
                "flooding_pop_id": "24,34,13,2,3,1,30,23,0,31,6,29,22,27,5,35,28,32,21,17,18,15,8",
                "redis_key": "rj_escritorio_dev_test_schedule_cached_flooding_occurences",
                "to_email": "gabriel.gazola@poli.ufrj.br",
            },
        ),
    ]
)
