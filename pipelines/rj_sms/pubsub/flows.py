# -*- coding: utf-8 -*-
"""
Pub-sub flow definition
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

from pipelines.constants import constants
from pipelines.rj_sms.pubsub.tasks import (
    acknowledge_message,
    get_messages_from_pubsub,
    log_message,
)
from pipelines.utils.decorators import Flow


with Flow(
    name="SMS: Pub/Sub - Consumir e exibir mensagens do Pub/Sub",
    code_owners=[
        "gabriel",
    ],
) as rj_sms_pubsub_print_flow:

    # Parameters
    subscription = Parameter("subscription", required=True)
    max_messages = Parameter("max_messages", default=1000)

    # Get messages from Pub/Sub
    messages = get_messages_from_pubsub(
        subscription=subscription, max_messages=max_messages
    )

    # Log messages
    logged_messages = log_message.map(message=messages)

    # Acknowledge messages
    acknowledge_message.map(
        message=logged_messages, subscription=unmapped(subscription)
    )

rj_sms_pubsub_print_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_sms_pubsub_print_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
