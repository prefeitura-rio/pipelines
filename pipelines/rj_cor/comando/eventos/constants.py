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
    TABLE_ID_EVENTOS = "ocorrencias_temp"
    TABLE_ID_ATIVIDADES_EVENTOS = "ocorrencias_orgaos_responsaveis_temp"
    TABLE_ID_POPS = "procedimento_operacional_padrao_temp"
    TABLE_ID_ATIVIDADES_POPS = (
        "procedimento_operacional_padrao_orgaos_responsaveis_temp"
    )
    RAIN_DASHBOARD_FLOW_SCHEDULE_PARAMETERS = {
        "query_data": """

        WITH
            alagamentos AS (
            SELECT
                id_evento,
                ST_GEOGPOINT(CAST(longitude AS FLOAT64),
                CAST(latitude AS FLOAT64)) AS geometry
            FROM `rj-cor.adm_cor_comando_staging.ocorrencias_temp`
            WHERE id_pop IN ("5", "6", "31", "32", "33")
            --    AND data_particao >= DATE_TRUNC(TIMESTAMP_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 15 MINUTE), day)
            --    AND CAST(data_inicio AS DATETIME) >= TIMESTAMP_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 15 MINUTE)
            ),
            final_table AS (
            SELECT
                h3_grid.id AS id_h3,
                nome AS bairro,
                COUNT(DISTINCT id_evento) AS qnt_alagamentos
            FROM `rj-cor.dados_mestres.h3_grid_res8` h3_grid
            INNER JOIN `rj-cor.dados_mestres.bairro`
                ON ST_CONTAINS(`rj-cor.dados_mestres.bairro`.geometry, ST_CENTROID(h3_grid.geometry))
            LEFT JOIN alagamentos
                ON ST_CONTAINS(h3_grid.geometry, alagamentos.geometry)
            GROUP BY id_h3, bairro
            )

        SELECT
            id_h3,
            bairro,
            qnt_alagamentos,
            CASE
                WHEN chuva_15min> 0.2  AND chuva_15min<= 1.25 THEN 'chuva fraca'
                WHEN chuva_15min> 1.25  AND chuva_15min<= 6.25 THEN 'chuva moderada'
                WHEN chuva_15min> 6.25  AND chuva_15min<= 12.5 THEN 'chuva forte'
                WHEN chuva_15min> 12.5                         THEN 'chuva muito forte'
                ELSE 'sem chuva'
            END AS status,
            CASE
                WHEN chuva_15min> 0     AND chuva_15min<= 1.25 THEN '#DAECFB'--'#00CCFF'
                WHEN chuva_15min> 1.25  AND chuva_15min<= 6.25 THEN '#A9CBE8'--'#BFA230'
                WHEN chuva_15min> 6.25  AND chuva_15min<= 12.5 THEN '#77A9D5'--'#E0701F'
                WHEN chuva_15min> 12.5                         THEN '#125999'--'#FF0000'
                ELSE '#ffffff'
            END AS color
        FROM final_table
        """,
        "query_update": """
        SELECT
            MAX(
            DATETIME(
                data_inicio
            )
            ) AS last_update
        FROM `rj-cor.adm_cor_comando_staging.ocorrencias_temp`
        --WHERE data_particao> DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 2 DAY)
        """,
    }
