# -*- coding: utf-8 -*-
from concurrent.futures import Future
from typing import MutableSequence

from google.cloud import pubsub
from google.pubsub_v1.types.pubsub import ReceivedMessage

from basedosdados.upload.base import Base
from pipelines.utils.utils import get_credentials_from_env


def ack_received_message(
    *,
    message: ReceivedMessage,
    subscription: str,
    project_id: str = None,
    subscriber_client: pubsub.SubscriberClient = None,
):
    """
    Acknowledge a received message
    """
    if project_id is None:
        base = Base()
        project_id = base.config["gcloud-projects"]["prod"]["name"]

    if subscriber_client is None:
        subscriber_client = get_subscriber_client()

    subscription_path = subscriber_client.subscription_path(project_id, subscription)

    subscriber_client.acknowledge(
        request={"subscription": subscription_path, "ack_ids": [message.ack_id]}
    )


def get_publisher_client() -> pubsub.PublisherClient:
    """
    Get a publisher client for Google Cloud Pub/Sub
    """
    return pubsub.PublisherClient(
        credentials=get_credentials_from_env(
            scopes=["https://www.googleapis.com/auth/pubsub"]
        )
    )


def get_subscriber_client() -> pubsub.SubscriberClient:
    """
    Get a subscriber client for Google Cloud Pub/Sub
    """
    return pubsub.SubscriberClient(
        credentials=get_credentials_from_env(
            scopes=["https://www.googleapis.com/auth/pubsub"]
        )
    )


def publish_message(
    *,
    topic: str,
    message: str,
    project_id: str = None,
    publisher_client: pubsub.PublisherClient = None,
) -> str:
    """
    Publish a message to a topic
    """
    if project_id is None:
        base = Base()
        project_id = base.config["gcloud-projects"]["prod"]["name"]

    if publisher_client is None:
        publisher_client = get_publisher_client()

    topic_path = publisher_client.topic_path(project_id, topic)

    future: Future = publisher_client.publish(topic_path, data=message.encode("utf-8"))

    return future.result()


def fetch_messages(
    *,
    subscription: str,
    project_id: str = None,
    subscriber_client: pubsub.SubscriberClient = None,
    max_messages: int = 1000,
) -> MutableSequence[ReceivedMessage]:
    """
    Fetch messages from a subscription
    """
    if project_id is None:
        base = Base()
        project_id = base.config["gcloud-projects"]["prod"]["name"]

    if subscriber_client is None:
        subscriber_client = get_subscriber_client()

    subscription_path = subscriber_client.subscription_path(project_id, subscription)

    response = subscriber_client.pull(
        request={"subscription": subscription_path, "max_messages": max_messages}
    )

    return response.received_messages
