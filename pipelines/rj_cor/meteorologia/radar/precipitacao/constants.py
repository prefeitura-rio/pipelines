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
                MAX(last_update) AS last_update
            FROM `rj-cor.clima_radar_staging.taxa_precipitacao_guaratiba`
        ),
        final_table AS (
            SELECT
                id_h3,
                bairro,
                CAST(predictions AS FLOAT64) AS chuva_15min,
                "Guaratiba" AS estacoes,
            FROM `rj-cor.clima_radar_staging.taxa_precipitacao_guaratiba` tx
            INNER JOIN last_update_date lud ON lud.last_update = tx.last_update
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
            DATETIME(MAX(last_update)) AS last_update
        FROM `rj-cor.clima_radar_staging.taxa_precipitacao_guaratiba`
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
                MAX(last_update) AS last_update
            FROM `rj-cor.clima_radar_staging.taxa_precipitacao_guaratiba`
        ),
        final_table AS (
            SELECT
                id_h3,
                bairro,
                CAST(predictions AS FLOAT64) AS chuva_15min,
                "Guaratiba" AS estacoes,
            FROM `rj-cor.clima_radar_staging.taxa_precipitacao_guaratiba` tx
            INNER JOIN last_update_date lud ON lud.last_update = tx.last_update
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
            DATETIME(MAX(last_update)) AS last_update
        FROM `rj-cor.clima_radar_staging.taxa_precipitacao_guaratiba`
        """,
    }
