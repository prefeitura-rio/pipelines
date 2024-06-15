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

    INEA_AGENT_LABEL = "inea"

    RJ_DATARIO_AGENT_LABEL = "datario"
    RJ_DATARIO_DEV_AGENT_LABEL = "datario-dev"

    RJ_ESCRITORIO_AGENT_LABEL = "rj-escritorio"
    RJ_ESCRITORIO_DEV_AGENT_LABEL = "rj-escritorio-dev"

    RJ_SMTR_AGENT_LABEL = "rj-smtr"
    RJ_SMTR_DEV_AGENT_LABEL = "rj-smtr-dev"

    RJ_SME_AGENT_LABEL = "rj-sme"

    RJ_SEGOVI_AGENT_LABEL = "rj-segovi"

    RJ_SEOP_AGENT_LABEL = "rj-seop"

    RJ_COR_AGENT_LABEL = "rj-cor"

    RJ_RIOAGUAS_AGENT_LABEL = "rj-rioaguas"

    RJ_SMFP_AGENT_LABEL = "rj-smfp"

    RJ_SMS_AGENT_LABEL = "rj-sms"

    RJ_SMS_DEV_AGENT_LABEL = "rj-sms-dev"

    RJ_SMI_AGENT_LABEL = "rj-smi"

    RJ_SECONSERVA_AGENT_LABEL = "rj-seconserva"

    RJ_CETRIO_AGENT_LABEL = "rj-cetrio"

    RJ_SETUR_AGENT_LABEL = "rj-setur"

    RJ_IPLANRIO_AGENT_LABEL = "rj-iplanrio"

    RJ_PGM_AGENT_LABEL = "rj-pgm"

    RJ_SMAC_AGENT_LABEL = "rj-smac"

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
        # "gabriel": {
        #     "user_id": "218800040137719809",
        #     "type": "user_nickname",
        # },
        "diego": {
            "user_id": "272581753829326849",
            "type": "user_nickname",
        },
        "joao": {
            "user_id": "692742616416256019",
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
        "rodrigo": {
            "user_id": "1031636163804545094",
            "type": "user_nickname",
        },
        "boris": {
            "user_id": "1109195532884262934",
            "type": "user_nickname",
        },
        "thiago": {
            "user_id": "404716070088343552",
            "type": "user_nickname",
        },
        "andre": {
            "user_id": "369657115012366336",
            "type": "user_nickname",
        },
        "rafaelpinheiro": {
            "user_id": "1131538976101109772",
            "type": "user_nickname",
        },
        "carolinagomes": {
            "user_id": "620000269392019469",
            "type": "user_nickname",
        },
        "karinappassos": {
            "user_id": "222842688117014528",
            "type": "user_nickname",
        },
        "danilo": {
            "user_id": "1147152438487416873",
            "type": "user_nickname",
        },
        "dados_smtr": {
            "user_id": "1056928259700445245",
            "type": "role",
        },
    }
