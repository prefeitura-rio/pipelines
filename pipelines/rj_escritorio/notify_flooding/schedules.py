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
                "flooding_pop_id": "31,6,5,32,33",
                "to_email": "waze-crisis@google.com,vitor.nogueira@cor.rio,gabriel.gazola@poli.ufrj.br",  # noqa
                "circle_radius": 50,
            },
        ),
    ]
)
