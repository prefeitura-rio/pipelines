"""
Constants for all flows
"""
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants used in the EMD flows.
    """

    ######################################
    # Automatically managed,
    # please do not change these values
    ######################################
    # Docker image
    DOCKER_TAG = "AUTO_REPLACE_DOCKER_TAG"
    DOCKER_IMAGE_NAME = "AUTO_REPLACE_DOCKER_IMAGE"
    DOCKER_IMAGE = f"{DOCKER_IMAGE_NAME}:{DOCKER_TAG}"
    # Prefect agents AUTO_FIND=M9w=k-b_
    PRECIPITACAO_ALERTARIO_AGENT_LABEL = "emd"
    EMD_AGENT_LABEL = "emd"

    RJ_ESCRITORIO_AGENT_LABEL = "rj-escritorio"
    RJ_ESCRITORIO_DEV_AGENT_LABEL = "rj-escritorio-dev"
    RJ_DATARIO_AGENT_LABEL = "datario"
    RJ_DATARIO_DEV_AGENT_LABEL = "datario-dev"

    RJ_SME_AGENT_LABEL = "rj-sme"
    RJ_SME_DEV_AGENT_LABEL = "rj-sme-dev"

    ######################################
    # Other constants
    ######################################
    # Discord
    EMD_DISCORD_WEBHOOK_SECRET_PATH = "prefect-discord-webhook"
    # Prefect
    K8S_AGENT_LABEL = "kubernetes"
    GCS_FLOWS_BUCKET = "datario-public"
    # Prefect tasks retry policy
    TASK_MAX_RETRIES = 5
    TASK_RETRY_DELAY = 10  # seconds
    # Telegram
    TELEGRAM_MAX_MESSAGE_LENGTH = 4096
