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

    RJ_SMI_AGENT_LABEL = "rj-smi"

    RJ_SECONSERVA_AGENT_LABEL = "rj-seconserva"

    ######################################
    # Other constants
    ######################################
    # Timezone
    DEFAULT_TIMEZONE = "America/Sao_Paulo"
    # Prefect
    K8S_AGENT_LABEL = "kubernetes"
    GCS_FLOWS_BUCKET = "datario-public"
    PREFECT_DEFAULT_PROJECT = "main"
    # Prefect tasks retry policy
    TASK_MAX_RETRIES = 5
    TASK_RETRY_DELAY = 10  # seconds
    # Telegram
    TELEGRAM_MAX_MESSAGE_LENGTH = 4096
    # MLflow
    MLFLOW_TRACKING_URI = "http://mlflow-tracking-server.mlflow.svc.cluster.local:5000"

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
            "user_id": "962067746651275304",
            "type": "role",
        },
        "gabriel": {
            "user_id": "218800040137719809",
            "type": "user_nickname",
        },
        "diego": {
            "user_id": "272581753829326849",
            "type": "user_nickname",
        },
        "fernanda": {
            "user_id": "692709168221650954",
            "type": "user_nickname",
        },
        "paty": {
            "user_id": "821121576455634955",
            "type": "user_nickname",
        },
        "bruno": {
            "user_id": "183691546942636033",
            "type": "user_nickname",
        },
        "caio": {
            "user_id": "276427674002522112",
            "type": "user_nickname",
        },
        "anderson": {
            "user_id": "553786261677015040",
            "type": "user_nickname",
        },
    }
