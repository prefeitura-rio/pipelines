# -*- coding: utf-8 -*-
"""
The daily birthday flow.
"""

from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio.birthdays.schedules import daily_at_9am
from pipelines.rj_escritorio.birthdays.tasks import (
    get_birthdays_by_date,
    get_todays_date,
    send_birthday_message,
)

with Flow("EMD: Aniversariante do dia") as birthday_flow:

    secret_path = Parameter("secret_path")

    today = get_todays_date()
    birthdays = get_birthdays_by_date(date=today)
    send_birthday_message(names=birthdays, secret_path=secret_path)

birthday_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
birthday_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
birthday_flow.schedule = daily_at_9am
