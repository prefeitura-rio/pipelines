# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Constants for the rain dashboard pipeline
"""
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants for the rain dashboard pipeline
    """

    DATASET_ID = "clima_radar"
    TABLE_ID = "taxa_precipitacao_guaratiba"
    RAIN_DASHBOARD_FLOW_NAME = (
        "EMD: Atualizar dados de chuva provinientes de radar na api.dados.rio"
    )
    RAIN_DASHBOARD_FLOW_SCHEDULE_PARAMETERS = {
        "redis_data_key": "data_chuva_recente_radar_inea",
        "redis_update_key": "data_update_chuva_recente_radar_inea",
        "query_data": """
        WITH
        last_update_date AS (
            SELECT
                MAX(data_medicao) AS last_update
            FROM `rj-cor.clima_radar_staging.taxa_precipitacao_guaratiba`
            WHERE data_particao>= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
        ),
        final_table AS (
            SELECT
                id_h3,
                case
                when id_h3 = "88a8a07735fffff" then "Barra da Tijuca"
                when id_h3 = "88a8a07ab5fffff" then "Ipanema"
                when id_h3 = "88a8a078e1fffff" then "Copacabana"
                when id_h3 = "88a8a03959fffff" then "Barra de Guaratiba"
                when id_h3 = "88a8a039d1fffff" then "Guaratiba"
                when id_h3 = "88a8a06817fffff" then "Ribeira"
                when id_h3 = "88a8a068e9fffff" then "Zumbi"
                else bairro end as bairro,
                CAST(predictions AS FLOAT64) AS chuva_15min,
                "Guaratiba" AS estacoes,
            FROM `rj-cor.clima_radar_staging.taxa_precipitacao_guaratiba` tx
            INNER JOIN last_update_date lud ON lud.last_update = tx.data_medicao
            WHERE id_h3 not in ("88a8a079ddfffff", "88a8a068e5fffff", "88a8a06995fffff")
        )
        SELECT
            id_h3,
            bairro,
            CASE
                WHEN chuva_15min<= 0.2 THEN 0.0 ELSE ROUND(chuva_15min, 2) END AS chuva_15min,
            estacoes,
            CASE
                WHEN chuva_15min> 0.2   AND chuva_15min<= 1.25 THEN 'chuva fraca'
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
            DATETIME(MAX(data_medicao)) AS last_update
        FROM `rj-cor.clima_radar_staging.taxa_precipitacao_guaratiba`
        WHERE data_particao>= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
        """,
    }
    # Modificar query para últimas 2h
    RAIN_DASHBOARD_LAST_2H_FLOW_SCHEDULE_PARAMETERS = {
        "redis_data_key": "data_chuva_passado_radar_inea",
        "redis_update_key": "data_update_chuva_passado_radar_inea",
        "query_data": """
        WITH
        last_update_date AS (
            SELECT
                CAST(MAX(data_medicao) AS DATETIME) AS last_update
            FROM `rj-cor.clima_radar_staging.taxa_precipitacao_guaratiba`
            WHERE data_particao>= CAST(DATE_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
        ),
        select_data_each_15_min AS (
            SELECT distinct
                id_h3,
                case
                when id_h3 = "88a8a07735fffff" then "Barra da Tijuca"
                when id_h3 = "88a8a07ab5fffff" then "Ipanema"
                when id_h3 = "88a8a078e1fffff" then "Copacabana"
                when id_h3 = "88a8a03959fffff" then "Barra de Guaratiba"
                when id_h3 = "88a8a039d1fffff" then "Guaratiba"
                when id_h3 = "88a8a06817fffff" then "Ribeira"
                when id_h3 = "88a8a068e9fffff" then "Zumbi"
                else bairro end as bairro,
                CASE WHEN
                    CAST(predictions AS FLOAT64) <= 0.2
                    THEN 0.0 ELSE CAST(predictions AS FLOAT64) END AS chuva_15min, -- retira ruídos antes de somar
                ROW_NUMBER() OVER (PARTITION BY id_h3, bairro ORDER BY data_medicao DESC) as row_num
            FROM `rj-cor.clima_radar_staging.taxa_precipitacao_guaratiba` tx
            INNER JOIN last_update_date lup ON 1=1
            WHERE tx.data_particao>= CAST(DATE_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
              AND CAST(tx.data_medicao AS DATETIME)>= DATE_SUB(lup.last_update, INTERVAL 2 HOUR)
              AND id_h3 not in ("88a8a079ddfffff", "88a8a068e5fffff", "88a8a06995fffff")
        ),
        final_table AS (
            SELECT
                id_h3,
                bairro,
                SUM(chuva_15min) AS chuva_15min,
                "Guaratiba" AS estacoes,
            FROM select_data_each_15_min
            WHERE row_num in (1, 4, 7, 10)
            GROUP BY id_h3, bairro, estacoes
        )
        SELECT
            id_h3,
            bairro,
            ROUND(chuva_15min, 2) AS chuva_15min,
            estacoes,
            CASE
                WHEN chuva_15min> 0   AND chuva_15min<= 10  THEN 'chuva fraca'
                WHEN chuva_15min> 10  AND chuva_15min<= 50  THEN 'chuva moderada'
                WHEN chuva_15min> 50  AND chuva_15min<= 100 THEN 'chuva forte'
                WHEN chuva_15min> 100                       THEN 'chuva muito forte'
                ELSE 'sem chuva'
            END AS status,
            CASE
                WHEN chuva_15min> 0  AND chuva_15min<= 10  THEN '#DAECFB'--'#00CCFF'
                WHEN chuva_15min> 1  AND chuva_15min<= 50  THEN '#A9CBE8'--'#BFA230'
                WHEN chuva_15min> 50 AND chuva_15min<= 100 THEN '#77A9D5'--'#E0701F'
                WHEN chuva_15min> 100                      THEN '#125999'--'#FF0000'
                ELSE '#ffffff'
            END AS color
        FROM final_table
        """,
        "query_update": """
        SELECT
            DATETIME(MAX(data_medicao)) AS last_update
        FROM `rj-cor.clima_radar_staging.taxa_precipitacao_guaratiba`
        WHERE data_particao>= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
        """,
    }
