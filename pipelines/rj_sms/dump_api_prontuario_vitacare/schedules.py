# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the vitacare dump pipeline
"""

from datetime import timedelta, datetime
from prefect.schedules.clocks import IntervalClock
from prefect.schedules import Schedule
import pytz
import pendulum


from pipelines.constants import constants
from pipelines.rj_sms.dump_api_prontuario_vitacare.constants import (
    constants as vitacare_constants,
)
from pipelines.utils.utils import untuple_clocks as untuple
from pipelines.rj_sms.utils import generate_dicts, generate_dump_api_schedules


posicao_parameters = generate_dicts(
    dict_template={
        "dataset_id": vitacare_constants.DATASET_ID.value,
        "table_id": "estoque_posicao",
        "ap": "",
        "endpoint": "posicao",
        "date": "today",
    },
    key="ap",
    values=["10", "21", "22", "31", "32", "33", "40", "51", "52", "53"],
)

movimento_parameters = generate_dicts(
    dict_template={
        "dataset_id": vitacare_constants.DATASET_ID.value,
        "table_id": "estoque_movimento",
        "ap": "",
        "endpoint": "movimento",
        "date": "yesterday",
    },
    key="ap",
    values=["10", "21", "22", "31", "32", "33", "40", "51", "52", "53"],
)

flow_parameters = posicao_parameters + movimento_parameters


vitacare_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=1,
)

vitacare_daily_update_schedule = Schedule(clocks=untuple(vitacare_clocks))

vitacare_every_day_at_six_am = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=pendulum.datetime(2023, 1, 1, 6, 0, 0, tz="America/Sao_Paulo"),
            labels=[
                constants.RJ_SMS_DEV_AGENT_LABEL.value,
            ],
        )
    ]
)

vitacare_every_day_at_seven_am = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=pendulum.datetime(2023, 1, 1, 7, 0, 0, tz="America/Sao_Paulo"),
            labels=[
                constants.RJ_SMS_DEV_AGENT_LABEL.value,
            ],
        )
    ]
)
