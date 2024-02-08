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
    mode: str, params: dict, code_owners: list = None, queries: dict = None
) -> bool:
    """
    Verify data quality for the subsídio process

    Args:
        mode (str): Execution mode (pre or post)
        params (dict): Parameters for the checks
        code_owners (list): Code owners to be notified
        queries (dict): SQL queries for the checks

    Returns:
        test_check (bool): True if all checks passed, False otherwise
    """

    if mode not in ["pre", "post"]:
        raise ValueError(f"Invalid mode: {mode}")

    if queries is None:
        queries = smtr_constants.SUBSIDIO_SPPO_DATA_CHECKS_QUERIES.value

    if code_owners is None:
        code_owners = ["rodrigo"]

    checks = list()

    if mode == "pre":
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
        check_params = queries.get("check_gps_capture")
        checks.append(
            perform_check(
                "Captura dos dados de GPS",
                check_params["query"].format(**request_params),
                check_params["order_columns"],
            )
        )

        request_params = general_request_params | {
            "interval": 10,
            "dataset_id": smtr_constants.GPS_SPPO_RAW_DATASET_ID.value,
            "table_id": smtr_constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value,
        }
        check_params = queries.get("check_gps_capture")
        checks.append(
            perform_check(
                "Captura dos dados de GPS - Realocação",
                check_params["query"].format(**request_params),
                check_params["order_columns"],
            )
        )

        check_params = queries.get("check_gps_treatment")
        checks.append(
            perform_check(
                "Tratamento dos dados de GPS",
                check_params["query"].format(**request_params),
                check_params["order_columns"],
            )
        )

        check_params = queries.get("check_sppo_veiculo_dia")
        checks.append(
            perform_check(
                "Tratamento da sppo_veiculo_dia",
                check_params["query"].format(**request_params),
                check_params["order_columns"],
            )
        )

        if mode == "post":
            # Adicionar testes após o subsídio ou adicionar testes do dia anterior
            return

    log(checks)

    date_range = (
        params["start_date"]
        if params["start_date"] == params["end_date"]
        else f'{params["start_date"]} a {params["end_date"]}'
    )
    formatted_messages = [
        f"**Data Quality Checks - Apuração de Subsídio - {date_range}**\n\n"
    ]
    test_check = True

    for check in checks:
        formatted_messages.append(
            f'{":white_check_mark:" if check["status"] else ":x:"} {check["desc"]}\n'
        )
        if not check["status"]:
            test_check = False

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
        formatted_messages.insert(0, ":red_circle: ")

        formatted_message = "".join(formatted_messages)

    else:
        formatted_messages.insert(0, ":green_circle: ")

    log(formatted_message)

    send_discord_message(
        message=formatted_message,
        webhook_url=get_vault_secret(
            secret_path=smtr_constants.SUBSIDIO_SPPO_SECRET_PATH.value
        )["data"]["discord_data_check_webhook"],
    )

    return test_check
