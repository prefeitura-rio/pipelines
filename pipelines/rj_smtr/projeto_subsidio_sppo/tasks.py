# -*- coding: utf-8 -*-
"""
Tasks for projeto_subsidio_sppo
"""

from prefect import task

import basedosdados as bd
import pandas as pd
from datetime import datetime, timedelta

from pipelines.constants import constants
from pipelines.utils.utils import (
    send_discord_message,
    log,
    get_vault_secret
)

from pipelines.rj_smtr.utils import data_info_str
from pipelines.rj_smtr.constants import constants as smtr_constants


@task
def check_param(param: str) -> bool:
    """
    Check if param is None
    """
    return param is None

@task
def subsidio_data_quality_check(
        mode: str,
        params: dict,
        code_owners: list = ["rodrigo"]
    ) -> None:
    """
    Verifica qualidade dos dados para apuração de subsídio

    Args:
        mode (str): Modo de verificação
        params (dict): Parâmetros

    Returns:
        None
    """

    checks = list()

    if mode == "pre":
        # Checagem de captura dos dados de GPS
        start_timestamp = f"""{params["start_date"]} 00:00:00"""
        end_timestamp = (datetime.strptime(params["end_date"],"%Y-%m-%d") + timedelta(hours=27)).strftime("%Y-%m-%d %H:%M:%S")
        q = f"""
            WITH
                t AS (
                SELECT
                    DATETIME(timestamp_array) AS timestamp_array
                FROM
                    UNNEST( GENERATE_TIMESTAMP_ARRAY( TIMESTAMP("{start_timestamp}"), TIMESTAMP("{end_timestamp}"), INTERVAL 1 minute) ) AS timestamp_array
                WHERE
                    timestamp_array < TIMESTAMP("{end_timestamp}") ),
                logs_table AS (
                SELECT
                    SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura,
                    SAFE_CAST(sucesso AS BOOLEAN) sucesso,
                    SAFE_CAST(erro AS STRING) erro,
                    SAFE_CAST(DATA AS DATE) DATA
                FROM
                    rj-smtr-staging.{smtr_constants.GPS_SPPO_RAW_DATASET_ID.value}_staging.{smtr_constants.GPS_SPPO_RAW_TABLE_ID.value}_logs AS t ),
                logs AS (
                SELECT
                    *,
                    TIMESTAMP_TRUNC(timestamp_captura, minute) AS timestamp_array
                FROM
                    logs_table
                WHERE
                    DATA BETWEEN DATE(TIMESTAMP("{start_timestamp}"))
                    AND DATE(TIMESTAMP("{end_timestamp}"))
                    AND timestamp_captura BETWEEN "{start_timestamp}"
                    AND "{end_timestamp}" )
                SELECT
                    COALESCE(logs.timestamp_captura, t.timestamp_array) AS timestamp_captura,
                    logs.erro
                FROM
                    t
                LEFT JOIN
                    logs
                ON
                    logs.timestamp_array = t.timestamp_array
                WHERE
                    logs.sucesso IS NOT TRUE
        """
        log(q)
        df = bd.read_sql(q)

        check_status = (len(df) == 0)

        check_status_dict = {"desc": "Captura dos dados de GPS",
                             "status": check_status}

        log(check_status_dict)

        if(check_status == False):
            log(data_info_str(df))
            log(df.sort_values(by=['timestamp_array']))

        checks.append(check_status_dict)

        # Checagem de captura dos dados de realocação de GPS
        start_timestamp = f"""{params["start_date"]} 00:00:00"""
        end_timestamp = (datetime.strptime(params["end_date"],"%Y-%m-%d") + timedelta(hours=27)).strftime("%Y-%m-%d %H:%M:%S")
        q = f"""
            WITH
                t AS (
                SELECT
                    DATETIME(timestamp_array) AS timestamp_array
                FROM
                    UNNEST( GENERATE_TIMESTAMP_ARRAY( TIMESTAMP("{start_timestamp}"), TIMESTAMP("{end_timestamp}"), INTERVAL 10 minute) ) AS timestamp_array
                WHERE
                    timestamp_array < TIMESTAMP("{end_timestamp}") ),
                logs_table AS (
                SELECT
                    SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura,
                    SAFE_CAST(sucesso AS BOOLEAN) sucesso,
                    SAFE_CAST(erro AS STRING) erro,
                    SAFE_CAST(DATA AS DATE) DATA
                FROM
                    rj-smtr-staging.{smtr_constants.GPS_SPPO_RAW_DATASET_ID.value}_staging.{smtr_constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value}_logs AS t ),
                logs AS (
                SELECT
                    *,
                    TIMESTAMP_TRUNC(timestamp_captura, minute) AS timestamp_array
                FROM
                    logs_table
                WHERE
                    DATA BETWEEN DATE(TIMESTAMP("{start_timestamp}"))
                    AND DATE(TIMESTAMP("{end_timestamp}"))
                    AND timestamp_captura BETWEEN "{start_timestamp}"
                    AND "{end_timestamp}" )
                SELECT
                    COALESCE(logs.timestamp_captura, t.timestamp_array) AS timestamp_captura,
                    logs.erro
                FROM
                    t
                LEFT JOIN
                    logs
                ON
                    logs.timestamp_array = t.timestamp_array
                WHERE
                    logs.sucesso IS NOT TRUE
        """
        log(q)
        df = bd.read_sql(q)

        check_status = (len(df) == 0)

        check_status_dict = {"desc": "Captura dos dados de realocação de GPS",
                             "status": check_status}

        log(check_status_dict)

        if(check_status == False):
            log(data_info_str(df))
            log(df.sort_values(by=['timestamp_array']))

        checks.append(check_status_dict)

        # Checagem do tratamento dos dados GPS
        q = f"""
            WITH
            data_hora AS (
                SELECT
                    EXTRACT(date
                    FROM
                    timestamp_array) AS DATA,
                    EXTRACT(hour
                    FROM
                    timestamp_array) AS hora,
                FROM
                    UNNEST(GENERATE_TIMESTAMP_ARRAY("{params["start_date"]} 00:00:00", "{params["end_date"]} 23:59:59", INTERVAL 1 hour)) AS timestamp_array ),
            gps_raw AS (
                SELECT
                    EXTRACT(date
                    FROM
                    timestamp_gps) AS DATA,
                    EXTRACT(hour
                    FROM
                    timestamp_gps) AS hora,
                    COUNT(*) AS q_gps_raw
                FROM
                    `rj-smtr.br_rj_riodejaneiro_onibus_gps.registros`
                WHERE
                    DATA BETWEEN "{params["start_date"]}"
                    AND "{params["end_date"]}"
                GROUP BY
                    1,
                    2 ),
            gps_filtrada AS (
                SELECT
                    DATA,
                    EXTRACT(hour
                    FROM
                    timestamp_gps) AS hora,
                    COUNT(*) AS q_gps_filtrada
                FROM
                    `rj-smtr.br_rj_riodejaneiro_onibus_gps.sppo_aux_registros_filtrada`
                WHERE
                    DATA BETWEEN "{params["start_date"]}"
                    AND "{params["end_date"]}"
                GROUP BY
                    1,
                    2 ),
            gps_realocacao AS (
                SELECT
                    DATA,
                    EXTRACT(hour
                    FROM
                    timestamp_gps) AS hora,
                    COUNT(*) AS q_gps_realocacao
                FROM
                    `rj-smtr.br_rj_riodejaneiro_onibus_gps.sppo_aux_registros_realocacao`
                WHERE
                    DATA BETWEEN "{params["start_date"]}"
                    AND "{params["end_date"]}"
                GROUP BY
                    1,
                    2 ),
            gps_sppo AS (
                SELECT
                    DATA,
                    EXTRACT(hour
                    FROM
                    timestamp_gps) AS hora,
                    COUNT(*) AS q_gps_treated
                FROM
                    `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
                WHERE
                    DATA BETWEEN "{params["start_date"]}"
                    AND "{params["end_date"]}"
                GROUP BY
                    1,
                    2),
            gps_join AS (
                SELECT
                    *,
                    CASE
                    WHEN (((q_gps_raw <= q_gps_filtrada) 
                            OR ((q_gps_filtrada+q_gps_realocacao) <= q_gps_treated))) 
                            OR q_gps_raw = 0 OR q_gps_filtrada = 0 
                            OR q_gps_treated = 0 
                            OR q_gps_raw IS NULL 
                            OR q_gps_filtrada IS NULL 
                            OR q_gps_treated IS NULL 
                            THEN FALSE
                    ELSE
                    TRUE
                END
                    AS status
                FROM
                    data_hora
                LEFT JOIN
                    gps_raw
                USING
                    (DATA,
                    hora)
                LEFT JOIN
                    gps_filtrada
                USING
                    (DATA,
                    hora)
                LEFT JOIN
                    gps_sppo
                USING
                    (DATA,
                    hora)
                LEFT JOIN
                    gps_realocacao
                USING
                    (DATA,
                    hora))
            SELECT
                *
            FROM
                gps_join
            WHERE
                status IS FALSE
            """

        log(q)
        df = bd.read_sql(q)

        check_status = (len(df) == 0)

        check_status_dict = {"desc": "Tratamento dos dados de GPS",
                             "status": check_status}

        log(check_status_dict)

        if(check_status == False):
            log(data_info_str(df))
            log(df.sort_values(by=['DATA', 'hora']))

        checks.append(check_status_dict)

        # Checagem do tratamento da sppo_veiculo_dia
        q = f"""
            WITH
                count_dist_status AS (
                SELECT
                    DATA,
                    COUNT(DISTINCT status) AS q_dist_status,
                    NULL AS q_duplicated_status,
                    NULL AS q_null_status
                FROM
                    rj-smtr.veiculo.sppo_veiculo_dia
                WHERE
                    DATA BETWEEN '{params["start_date"]}'
                    AND '{params["end_date"]}'
                GROUP BY
                    1
                HAVING
                    COUNT(DISTINCT status) = 1 ),
                count_duplicated_status AS (
                SELECT
                    DATA,
                    id_veiculo,
                    COUNT(*) AS q_status,
                FROM
                    rj-smtr.veiculo.sppo_veiculo_dia
                WHERE
                    DATA BETWEEN '{params["start_date"]}'
                    AND '{params["end_date"]}'
                GROUP BY
                    1,
                    2
                HAVING
                    COUNT(*) > 1 ),
                count_duplicated_status_agg AS (
                SELECT
                    DATA,
                    NULL AS q_dist_status,
                    SUM(q_status) AS q_duplicated_status,
                    NULL AS q_null_status
                FROM
                    count_duplicated_status
                GROUP BY
                    1),
                count_null_status AS (
                SELECT
                    DATA,
                    NULL AS q_dist_status,
                    NULL AS q_duplicated_status,
                    COUNT(*) AS q_null_status
                FROM
                    rj-smtr.veiculo.sppo_veiculo_dia
                WHERE
                    DATA BETWEEN '{params["start_date"]}'
                    AND '{params["end_date"]}'
                    AND status IS NULL
                GROUP BY
                    1 )
            SELECT
                *
            FROM
                count_dist_status
            
            UNION ALL
            
            SELECT
                *
            FROM
                count_duplicated_status_agg
            
            UNION ALL

            SELECT
                *
            FROM
                count_null_status
            """

        log(q)
        df = bd.read_sql(q)

        check_status = (len(df) == 0)

        check_status_dict = {"desc": "Tratamento da sppo_veiculo_dia",
                             "status": check_status}

        log(check_status_dict)

        if(check_status == False):
            log(data_info_str(df))
            log(df.sort_values(by=['DATA']))

        checks.append(check_status_dict)

    if mode == "post":
        # Adicionar testes após o subsídio ou adicionar testes do dia anterior
        return

    log(checks)

    if params["start_date"] == params["end_date"]: 
        date_range = params["start_date"] 
    else: 
        date_range = f"""{params["start_date"]} a {params["end_date"]}"""

    formatted_message = f"""**Data Quality Checks - Apuração de Subsídio - {date_range}**\n\n"""
    call_admin = False
    for check in checks:
        formatted_message += f"""{":white_check_mark:" if check["status"] is True else ":x:"} {check["desc"]}\n"""
        if check["status"] is False:
            call_admin = True

    at_code_owners = []
    if call_admin:
        for code_owner in code_owners:
            code_owner_id = constants.OWNERS_DISCORD_MENTIONS.value[code_owner]["user_id"]
            code_owner_type = constants.OWNERS_DISCORD_MENTIONS.value[code_owner]["type"]

            if code_owner_type == "user":
                at_code_owners.append(f"    - <@{code_owner_id}>\n")
            elif code_owner_type == "user_nickname":
                at_code_owners.append(f"    - <@!{code_owner_id}>\n")
            elif code_owner_type == "channel":
                at_code_owners.append(f"    - <#{code_owner_id}>\n")
            elif code_owner_type == "role":
                at_code_owners.append(f"    - <@&{code_owner_id}>\n")
        
        formatted_message += "".join(at_code_owners)

        formatted_message = f":red_circle: {formatted_message}"
    else:
        formatted_message = f":green_circle: {formatted_message}"

    log(formatted_message)

    send_discord_message(
        message=formatted_message,
        webhook_url=get_vault_secret(secret_path=smtr_constants.SUBSIDIO_SPPO_SECRET_PATH.value)["data"]["discord_data_check_webhook"],
    ) 