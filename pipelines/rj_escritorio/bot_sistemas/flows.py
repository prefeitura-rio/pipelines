# -*- coding: utf-8 -*-
"""
Flow definition for the systems bot.
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_cor.bot_semaforo.tasks import (
    get_token_and_group_id,
)
from pipelines.rj_escritorio.bot_sistemas.schedules import systems_telegram_bot_schedule
from pipelines.rj_escritorio.bot_sistemas.tasks import (
    build_message,
    get_data,
    preprocess_data,
    split_and_send_messages,
)
from pipelines.utils.decorators import Flow

with Flow(
    name="EMD: Sistemas prioritários - Telegram Bot",
    code_owners=[
        "gabriel",
    ],
) as systems_telegram_bot_flow:
    # Parameters
    sheet_id = Parameter("sheet_id")
    sheet_name = Parameter("sheet_name")
    secret_path = Parameter("secret_path")

    # Get credentials for Telegram
    token, group_id = get_token_and_group_id(secret_path=secret_path)

    # Get data and generate messages
    dataframe = get_data(sheet_id=sheet_id, sheet_name=sheet_name)
    preproc_data = preprocess_data(dataframe=dataframe)
    message = build_message(data=preproc_data)

    # Send messages
    split_and_send_messages(
        token=token,
        group_id=group_id,
        message=message,
    )

systems_telegram_bot_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
systems_telegram_bot_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
)
systems_telegram_bot_flow.schedule = systems_telegram_bot_schedule
