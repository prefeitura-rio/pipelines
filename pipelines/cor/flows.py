"""
Flows for cor
"""

from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.cor.schedules import bot_schedule
from pipelines.cor.tasks import (
    get_token_and_group_id,
    get_data,
    format_message,
    send_messages,
)

with Flow("COR: CET semáforos - Telegram Bot") as cet_telegram_flow:

    secret_path = Parameter("secret_path")

    # Get credentials for Telegram
    token, group_id = get_token_and_group_id(secret_path=secret_path)

    # Get data and generate messages
    dataframe = get_data()
    messages = format_message(
        dataframe=dataframe)

    # Send messages
    send_messages(
        token=token,
        group_id=group_id,
        messages=messages,
    )

cet_telegram_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cet_telegram_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value)
cet_telegram_flow.schedule = bot_schedule
