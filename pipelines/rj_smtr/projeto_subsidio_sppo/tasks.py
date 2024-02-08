# -*- coding: utf-8 -*-
"""
Tasks for projeto_subsidio_sppo
"""

from prefect import task

import basedosdados as bd
import pandas as pd
from datetime import datetime, timedelta

from pipelines.constants import constants
from pipelines.utils.utils import send_discord_message, log, get_vault_secret

from pipelines.rj_smtr.utils import perform_check
from pipelines.rj_smtr.constants import constants as smtr_constants


@task
def check_param(param: str) -> bool:
    """
    Check if param is None
    """
    return param is None


@task
def subsidio_data_quality_check(
    params: dict, code_owners: list = None, check_params: dict = None
) -> bool:
    """
    Verify data quality for the subsídio process

    Args:
        params (dict): Parameters for the checks
        code_owners (list): Code owners to be notified
        check_params (dict): queries and order columns for the checks

    Returns:
        test_check (bool): True if all checks passed, False otherwise
    """

    if check_params is None:
        check_params = smtr_constants.SUBSIDIO_SPPO_DATA_CHECKS_PARAMS.value

    if code_owners is None:
        code_owners = ["rodrigo"]

    checks = list()

    general_request_params = {
        "start_timestamp": f"""{params["start_date"]} 00:00:00""",
        "end_timestamp": (
            datetime.strptime(params["end_date"], "%Y-%m-%d") + timedelta(hours=27)
        ).strftime("%Y-%m-%d %H:%M:%S"),
    }

    request_params = general_request_params | {
        "interval": 1,
        "dataset_id": smtr_constants.GPS_SPPO_RAW_DATASET_ID.value,
        "table_id": smtr_constants.GPS_SPPO_RAW_TABLE_ID.value,
    }
    checks.append(
        perform_check(
            "Captura dos dados de GPS",
            check_params.get("check_gps_capture"),
            request_params,
        )
    )

    request_params = general_request_params | {
        "interval": 10,
        "dataset_id": smtr_constants.GPS_SPPO_RAW_DATASET_ID.value,
        "table_id": smtr_constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value,
    }
    checks.append(
        perform_check(
            "Captura dos dados de GPS - Realocação",
            check_params.get("check_gps_capture"),
            request_params,
        )
    )

    checks.append(
        perform_check(
            "Tratamento dos dados de GPS",
            check_params.get("check_gps_treatment"),
            general_request_params,
        )
    )

    checks.append(
        perform_check(
            "Tratamento da sppo_veiculo_dia",
            check_params.get("check_sppo_veiculo_dia"),
            general_request_params,
        )
    )

    log(checks)

    date_range = (
        params["start_date"]
        if params["start_date"] == params["end_date"]
        else f'{params["start_date"]} a {params["end_date"]}'
    )
    formatted_messages = [
        f"**Data Quality Checks - Apuração de Subsídio - {date_range}**\n\n"
    ]
    test_check = all(check["status"] for check in checks)

    for check in checks:
        formatted_messages.append(
            f'{":white_check_mark:" if check["status"] else ":x:"} {check["desc"]}\n'
        )

    if not test_check:
        at_code_owners = [
            f'    - <@{constants.OWNERS_DISCORD_MENTIONS.value[code_owner]["user_id"]}>\n'
            if constants.OWNERS_DISCORD_MENTIONS.value[code_owner]["type"] == "user"
            else f'    - <@!{constants.OWNERS_DISCORD_MENTIONS.value[code_owner]["user_id"]}>\n'
            if constants.OWNERS_DISCORD_MENTIONS.value[code_owner]["type"]
            == "user_nickname"
            else f'    - <#{constants.OWNERS_DISCORD_MENTIONS.value[code_owner]["user_id"]}>\n'
            if constants.OWNERS_DISCORD_MENTIONS.value[code_owner]["type"] == "channel"
            else f'    - <@&{constants.OWNERS_DISCORD_MENTIONS.value[code_owner]["user_id"]}>\n'
            for code_owner in code_owners
        ]

        formatted_messages.extend(at_code_owners)

    formatted_messages.insert(0, ":green_circle: " if test_check else ":red_circle: ")
    formatted_message = "".join(formatted_messages)

    log(formatted_message)

    send_discord_message(
        message=formatted_message,
        webhook_url=get_vault_secret(
            secret_path=smtr_constants.SUBSIDIO_SPPO_SECRET_PATH.value
        )["data"]["discord_data_check_webhook"],
    )

    return test_check
