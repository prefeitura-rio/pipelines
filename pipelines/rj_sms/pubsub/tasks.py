# -*- coding: utf-8 -*-
from typing import MutableSequence

from google.pubsub_v1.types.pubsub import ReceivedMessage
from prefect import task

from pipelines.rj_sms.pubsub.utils import ack_received_message, fetch_messages
from pipelines.utils.utils import log


@task(checkpoint=False)
def get_messages_from_pubsub(
    subscription: str, max_messages: int = 1000
) -> MutableSequence[ReceivedMessage]:
    """
    Get messages from a Google Cloud Pub/Sub subscription
    """
    return fetch_messages(subscription=subscription, max_messages=max_messages)


@task(checkpoint=False)
def log_message(message: ReceivedMessage) -> ReceivedMessage:
    """
    Log a message from a Google Cloud Pub/Sub subscription
    """
    log(f"Message: {message.message.data.decode('utf-8')}")
    return message


@task
def acknowledge_message(message: ReceivedMessage, subscription: str):
    """
    Acknowledge a message from a Google Cloud Pub/Sub subscription
    """
    log(f"Acknowledging message: {message.message.data.decode('utf-8')}")
    ack_received_message(message=message, subscription=subscription)
