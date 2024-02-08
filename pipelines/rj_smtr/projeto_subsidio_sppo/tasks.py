# -*- coding: utf-8 -*-
"""
Tasks for projeto_subsidio_sppo
"""

from prefect import task

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
    mode: str, params: dict, code_owners: list = None, check_params: dict = None
) -> bool:
    """
    Verifica qualidade de dados para o processo de apuração de subsídio

    Args:
        params (dict): Parameters for the checks
        code_owners (list): Code owners to be notified
        check_params (dict): queries and order columns for the checks

    Returns:
        test_check (bool): True if all checks passed, False otherwise
    """

    if mode not in ["pre", "pos"]:
        raise ValueError("Mode must be 'pre' or 'pos'")

    if check_params is None:
        check_params = smtr_constants.SUBSIDIO_SPPO_DATA_CHECKS_PARAMS.value

    if code_owners is None:
        code_owners = smtr_constants.SUBSIDIO_SPPO_CODE_OWNERS.value

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

        general_request_params["end_timestamp"] = f"""{params["end_date"]} 00:00:00"""
        checks.append(
            perform_check(
                "Tratamento da sppo_veiculo_dia",
                check_params.get("check_sppo_veiculo_dia"),
                general_request_params,
            )
        )

    if mode == "pos":
        request_params = {
            "start_timestamp": f"""{params["start_date"]} 00:00:00""",
            "end_timestamp": f"""{params["end_date"]} 00:00:00""",
            "dataset_id": smtr_constants.SUBSIDIO_SPPO_DASHBOARD_DATASET_ID.value,
            "table_id": smtr_constants.SUBSIDIO_SPPO_DASHBOARD_TABLE_ID.value,
        }

        test_check_list = {
            "Checagem de Data": {"test": "sumario_servico_dia_checa_data"},
            "Valores de penalidade aceitos": {
                "test": "accepted_values_sumario_servico_dia_valor_penalidade"
            },
            "Teto de Pagamento de Valor de Subsídio": {
                "test": "teto_pagamento_sumario_servico_dia_valor_subsidio_pago"
            },
            "Unicidade da chave data-servico": {
                "test": "unique_combination",
                "expression": "data, servico",
            },
            "Data não é nulo": {
                "expression": "data IS NOT NULL",
            },
            "Tipo de dia não é nulo": {
                "expression": "tipo_dia IS NOT NULL",
            },
            "Consórcio não é nulo": {
                "expression": "consorcio IS NOT NULL",
            },
            "Serviço não é nulo": {
                "expression": "servico IS NOT NULL",
            },
            "Quantidade de Viagens não nulo e maior ou igual a zero": {
                "expression": "viagens IS NOT NULL AND viagens >= 0",
            },
            "Quilometragem Apurada não nulo e maior ou igual a zero": {
                "expression": "km_apurada IS NOT NULL AND km_apurada >= 0",
            },
            "Quilometragem Planejada não nulo e maior ou igual a zero": {
                "expression": "km_planejada IS NOT NULL AND km_planejada >= 0",
            },
            "Percentual de Operação Diário não nulo e maior ou igual a zero": {
                "expression": "perc_km_planejada IS NOT NULL AND perc_km_planejada >= 0",
            },
            "Valor de Subsídio Pago não nulo e maior ou igual a zero": {
                "expression": "valor_subsidio_pago IS NOT NULL AND valor_subsidio_pago >= 0",
            },
        }

        for description, test_check in test_check_list.items():
            request_params["expression"] = test_check.get("expression", "")
            checks.append(
                perform_check(
                    f"sumario_servico_dia - {description}",
                    check_params.get(test_check.get("test", "expression_is_true")),
                    request_params,
                )
            )

    log(checks)

    date_range = (
        params["start_date"]
        if params["start_date"] == params["end_date"]
        else f'{params["start_date"]} a {params["end_date"]}'
    )
    formatted_messages = [
        f"**{mode.capitalize()}-Data Quality Checks - Apuração de Subsídio - {date_range}**\n\n"
    ]
    test_check = all(check["status"] for check in checks)

    for check in checks:
        formatted_messages.append(
            f'{":white_check_mark:" if check["status"] else ":x:"} {check["desc"]}\n'
        )

    if mode == "pre":
        formatted_messages.append(
            ""
            if test_check
            else """:warning: **Status:** Run cancelada.
            Necessidade de revisão dos dados de entrada!\n"""
        )

    if mode == "pos":
        formatted_messages.append(
            ":tada: **Status:** Sucesso"
            if test_check
            else ":warning: **Status:** Testes falharam. Necessidade de revisão dos dados finais!\n"
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
