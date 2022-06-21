# -*- coding: utf-8 -*-
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

    RJ_DATARIO_AGENT_LABEL = "datario"
    RJ_DATARIO_DEV_AGENT_LABEL = "datario-dev"

    RJ_ESCRITORIO_AGENT_LABEL = "rj-escritorio"
    RJ_ESCRITORIO_DEV_AGENT_LABEL = "rj-escritorio-dev"

    RJ_SMTR_AGENT_LABEL = "rj-smtr"
    RJ_SMTR_DEV_AGENT_LABEL = "rj-smtr-dev"

    RJ_SME_AGENT_LABEL = "rj-sme"

    RJ_SEGOVI_AGENT_LABEL = "rj-segovi"

    RJ_COR_AGENT_LABEL = "rj-cor"

    RJ_SMFP_AGENT_LABEL = "rj-smfp"

    RJ_SECONSERVA_AGENT_LABEL = "rj-seconserva"

    ######################################
    # Other constants
    ######################################
    # Prefect
    K8S_AGENT_LABEL = "kubernetes"
    GCS_FLOWS_BUCKET = "datario-public"
    PREFECT_DEFAULT_PROJECT = "main"
    # Prefect tasks retry policy
    TASK_MAX_RETRIES = 5
    TASK_RETRY_DELAY = 10  # seconds
    # Telegram
    TELEGRAM_MAX_MESSAGE_LENGTH = 4096

    ######################################
    # Discord code owners constants
    ######################################
    EMD_DISCORD_WEBHOOK_SECRET_PATH = "prefect-discord-webhook"
    DEFAULT_CODE_OWNERS = ["pipeliners"]
    OWNERS_DISCORD_MENTIONS = {
        # Register all code owners, users_id and type
        #     - possible types: https://docs.discord.club/embedg/reference/mentions
        #     - how to discover user_id: https://www.remote.tools/remote-work/how-to-find-discord-id
        #     - types: user, user_nickname, channel, role
        "pipeliners": {
            "user_id": "865223885031997455",
            "type": "role",
        },
        "gabriel": {
            "user_id": "865034571469160458",
            "type": "user_nickname",
        },
        "diego": {
            "user_id": "272581753829326849",
            "type": "user_nickname",
        },
        "fernanda": {
            "user_id": "776914459545436200",
            "type": "user_nickname",
        },
        "paty": {
            "user_id": "740986161652301886",
            "type": "user_nickname",
        },
        "bruno": {
            "user_id": "467788821527003136",
            "type": "user_nickname",
        },
        "caio": {
            "user_id": "467788821527003136",
            "type": "user_nickname",
        },
        "anderson": {
            "user_id": "467788821527003136",
            "type": "user_nickname",
        },
    }
