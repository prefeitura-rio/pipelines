# -*- coding: utf-8 -*-
"""
Flows for sending messages through a Whatsapp Bot.
This module assumes that the bot inherits from the template bot at
https://github.com/prefeitura-rio/whatsapp-bot
"""

from functools import partial

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.utils import notify_discord_on_failure
from pipelines.utils.whatsapp_bot.tasks import (
    get_whatsapp_bot_api_url,
    send_whatsapp_message,
)

with Flow(
    name=utils_constants.FLOW_SEND_WHATSAPP_MESSAGE_NAME.value,
    code_owners=[
        "@Gabriel Gazola Milan#8183",
    ],
) as whatsapp_bot_send_message_flow:

    #####################################
    #
    # Parameters
    #
    #####################################

    secret_path = Parameter("secret_path")
    message = Parameter("message")
    chat_id = Parameter("chat_id")

    #####################################
    #
    # Tasks
    #
    #####################################

    bot_api_url = get_whatsapp_bot_api_url(secret_path=secret_path)
    send_whatsapp_message(
        bot_api_url=bot_api_url,
        message=message,
        chat_id=chat_id,
    )

whatsapp_bot_send_message_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
whatsapp_bot_send_message_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
