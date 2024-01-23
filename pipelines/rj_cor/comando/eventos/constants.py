# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Constant values for the rj_cor.comando projects
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the comando project
    """

    PATH_BASE_ENDERECOS = "/tmp/base_enderecos.csv"
    DATASET_ID = "adm_cor_comando"
    TABLE_ID_EVENTOS = "ocorrencias_nova_api"
    REDIS_NAME = "cor_api_last_days"
    # TABLE_ID_ATIVIDADES_EVENTOS = "ocorrencias_orgaos_responsaveis"
    # TABLE_ID_POPS = "procedimento_operacional_padrao"
    # TABLE_ID_ATIVIDADES_POPS = "procedimento_operacional_padrao_orgaos_responsaveis"
    RAIN_DASHBOARD_FLOW_SCHEDULE_PARAMETERS = {
        "redis_data_key": "data_alagamento_recente_comando",
        "redis_update_key": "data_update_alagamento_recente_comando",
        "query_data": """
        WITH
            alagamentos AS (
            SELECT
                id_evento,
                CASE WHEN id_pop IN ("6", "31", "32") THEN 3 -- "Alagamento"
                WHEN id_pop = "5" THEN 2 -- "Bolsão d'água"
                WHEN id_pop = "33" THEN 1 -- "Lâmina d'água"
                END AS tipo,
                ST_GEOGPOINT(CAST(longitude AS FLOAT64),
                CAST(latitude AS FLOAT64)) AS geometry
            FROM `rj-cor.adm_cor_comando_staging.ocorrencias`
            WHERE id_pop IN ("5", "6", "31", "32", "33")
                AND data_particao >= DATE_TRUNC(TIMESTAMP_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 1 day), day)
                AND CAST(data_inicio AS DATETIME) >= TIMESTAMP_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 1 day)
                AND data_fim IS NULL
            ),
            final_table AS (
            SELECT
                h3_grid.id AS id_h3,
                nome AS bairro,
                COALESCE(MAX(tipo), 0) AS tipo
            FROM `rj-cor.dados_mestres.h3_grid_res8` h3_grid
            LEFT JOIN `rj-cor.dados_mestres.bairro`
                ON ST_CONTAINS(`rj-cor.dados_mestres.bairro`.geometry, ST_CENTROID(h3_grid.geometry))
            LEFT JOIN alagamentos
                ON ST_CONTAINS(h3_grid.geometry, alagamentos.geometry)
            GROUP BY id_h3, bairro
            )

        SELECT
            id_h3,
            bairro,
            tipo AS qnt_alagamentos,
            CASE
                WHEN tipo = 3 THEN "extremamente crítico" --"Alagamento"
                WHEN tipo = 2 THEN "crítico" -- "Bolsão d'água"
                WHEN tipo = 1 THEN "pouco crítico" --"Lâmina d'água"
                ELSE "sem alagamento"
                END AS status,
            CASE
                WHEN tipo = 1 THEN '#DAECFB'--'#00CCFF'
                WHEN tipo = 2 THEN '#A9CBE8'--'#BFA230'
                WHEN tipo = 3 THEN '#125999'--'#E0701F'
                ELSE '#ffffff'
            END AS color
        FROM final_table
        """,
        "query_update": """
        SELECT date_trunc(current_datetime("America/Sao_Paulo"), minute) AS last_update
        """,
    }
    RAIN_DASHBOARD_LAST_2H_FLOW_SCHEDULE_PARAMETERS = {
        "redis_data_key": "data_alagamento_passado_comando",
        "redis_update_key": "data_update_alagamento_passado_comando",
        "query_data": """
        WITH
            alagamentos AS (
            SELECT
                id_evento,
                CASE WHEN id_pop IN ("6", "31", "32") THEN 3 -- "Alagamento"
                WHEN id_pop = "5" THEN 2 -- "Bolsão d'água"
                WHEN id_pop = "33" THEN 1 -- "Lâmina d'água"
                END AS tipo,
                ST_GEOGPOINT(CAST(longitude AS FLOAT64),
                CAST(latitude AS FLOAT64)) AS geometry
            FROM `rj-cor.adm_cor_comando_staging.ocorrencias`
            WHERE id_pop IN ("5", "6", "31", "32", "33")
                AND data_particao >= DATE_TRUNC(TIMESTAMP_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 120 MINUTE), day)
                AND (CAST(data_fim AS DATETIME) >= TIMESTAMP_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 120 MINUTE)
                    OR data_fim IS NULL)
            --    AND data_particao >= DATE_TRUNC(TIMESTAMP_SUB(CAST("2022-04-01 04:39:15" as datetime), INTERVAL 120 MINUTE), day)
            --    AND (CAST(data_fim AS DATETIME) >= TIMESTAMP_SUB(CAST("2022-04-01 04:39:15" as datetime), INTERVAL 120 MINUTE)
            --        OR data_fim IS NULL)
            ),
            final_table AS (
            SELECT
                h3_grid.id AS id_h3,
                nome AS bairro,
                COALESCE(MAX(tipo), 0) AS tipo
            FROM `rj-cor.dados_mestres.h3_grid_res8` h3_grid
            LEFT JOIN `rj-cor.dados_mestres.bairro`
                ON ST_CONTAINS(`rj-cor.dados_mestres.bairro`.geometry, ST_CENTROID(h3_grid.geometry))
            LEFT JOIN alagamentos
                ON ST_CONTAINS(h3_grid.geometry, alagamentos.geometry)
            GROUP BY id_h3, bairro
            )

        SELECT
            id_h3,
            bairro,
            CASE
                WHEN tipo = 3 THEN "Alagamento"
                WHEN tipo = 2 THEN "Bolsão d'água"
                WHEN tipo = 1 THEN "Lâmina d'água"
                ELSE "sem alagamento"
                END AS qnt_alagamentos,
            CASE
                WHEN tipo = 3 THEN "extremamente crítico" --"Alagamento"
                WHEN tipo = 2 THEN "crítico" -- "Bolsão d'água"
                WHEN tipo = 1 THEN "pouco crítico" --"Lâmina d'água"
                ELSE "sem alagamento"
                END AS status,
            CASE
                WHEN tipo = 1 THEN '#DAECFB'--'#00CCFF'
                WHEN tipo = 2 THEN '#A9CBE8'--'#BFA230'
                WHEN tipo = 3 THEN '#125999'--'#E0701F'
                ELSE '#ffffff'
            END AS color
        FROM final_table
        """,
        "query_update": """
        SELECT date_trunc(current_datetime("America/Sao_Paulo"), minute) AS last_update
        """,
    }
