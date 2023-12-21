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

    RAIN_DASHBOARD_FLOW_NAME = "EMD: Atualizar dados de chuva na api.dados.rio"
    RAIN_DASHBOARD_FLOW_SCHEDULE_PARAMETERS = {
        "query_data": """
        WITH
            alertario AS ( -- seleciona a última medição do alertario
            SELECT
                id_estacao,
                acumulado_chuva_15_min,
                CURRENT_DATE('America/Sao_Paulo') as data,
                data_update
            FROM (
                SELECT
                id_estacao,
                acumulado_chuva_15_min,
                data_particao,
                DATETIME(CONCAT(data_particao," ", horario)) AS data_update,
                ROW_NUMBER() OVER (
                    PARTITION BY id_estacao ORDER BY DATETIME(CONCAT(data_particao," ", horario)) DESC
                ) AS row_num
                FROM `rj-cor.clima_pluviometro.taxa_precipitacao_alertario`
                WHERE data_particao> DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY)
            )AS a
            WHERE a.row_num = 1
            ),

            websirene AS ( -- seleciona a última medição do websirene
            SELECT
                id_estacao,
                acumulado_chuva_15_min,
                CURRENT_DATE('America/Sao_Paulo') as data,
                data_update
            FROM (
                SELECT
                id_estacao,
                acumulado_chuva_15_min,
                data_particao,
                DATETIME(CONCAT(data_particao," ", horario)) AS data_update,
                ROW_NUMBER() OVER (
                    PARTITION BY id_estacao ORDER BY DATETIME(CONCAT(data_particao," ", horario)) DESC
                ) AS row_num
                FROM `rj-cor.clima_pluviometro.taxa_precipitacao_websirene`
                WHERE data_particao> DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY)
            )AS a
            WHERE a.row_num = 1
            ),

            cemaden AS ( -- seleciona a última medição do cemaden
            SELECT
                id_estacao,
                acumulado_chuva_10_min AS acumulado_chuva_15_min,
                CURRENT_DATE('America/Sao_Paulo') as data,
                data_update
            FROM (
                SELECT
                id_estacao,
                acumulado_chuva_10_min,
                data_particao,
                DATETIME(data_medicao) AS data_update,
                ROW_NUMBER() OVER (
                    PARTITION BY id_estacao ORDER BY DATETIME(data_medicao) DESC
                ) AS row_num
                FROM `rj-cor.clima_pluviometro.taxa_precipitacao_cemaden`
                WHERE data_particao> DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY)
            )AS a
            WHERE a.row_num = 1
            ),

            last_measurements AS (-- concatena medições do alertario, cemaden e websirene
            (SELECT
                id_estacao,
                data_update,
                "alertario" AS sistema,
                acumulado_chuva_15_min,
            FROM alertario)
            UNION ALL
            (SELECT
                id_estacao,
                data_update,
                "websirene" AS sistema,
                acumulado_chuva_15_min,
            FROM websirene)
            UNION ALL
            (SELECT
                id_estacao,
                data_update,
                "cemaden" AS sistema,
                acumulado_chuva_15_min,
            FROM cemaden)
            ),

            h3_chuvas AS ( -- calcula qnt de chuva para cada h3
            SELECT
                h3.*,
                lm.id_estacao,
                lm.acumulado_chuva_15_min,
                lm.acumulado_chuva_15_min/power(h3.dist,5) AS p1_15min,
                1/power(h3.dist,5) AS inv_dist
            FROM (
                WITH centroid_h3 AS (
                    SELECT
                        *,
                        ST_CENTROID(geometry) AS geom
                    FROM `rj-cor.dados_mestres.h3_grid_res8`
                ),

                estacoes_pluviometricas AS (
                    (SELECT
                        id_estacao AS id,
                        estacao,
                        "alertario" AS sistema,
                        ST_GEOGPOINT(CAST(longitude AS FLOAT64),
                        CAST(latitude AS FLOAT64)) AS geom
                    FROM `rj-cor.clima_pluviometro.estacoes_alertario`)
                    UNION ALL
                    (SELECT
                        id_estacao AS id,
                        estacao,
                        "websirene" AS sistema,
                        ST_GEOGPOINT(CAST(longitude AS FLOAT64),
                        CAST(latitude AS FLOAT64)) AS geom
                    FROM `rj-cor.clima_pluviometro.estacoes_websirene`)
                    UNION ALL
                    (SELECT
                        id_estacao AS id,
                        estacao,
                        "cemaden" AS sistema,
                        ST_GEOGPOINT(CAST(longitude AS FLOAT64),
                        CAST(latitude AS FLOAT64)) AS geom
                    FROM `rj-cor.clima_pluviometro.estacoes_cemaden`)
                ),

                estacoes_mais_proximas AS (
                    SELECT AS VALUE s
                    FROM (
                        SELECT
                            ARRAY_AGG(
                                STRUCT<id_h3 STRING,
                                id_estacao STRING,
                                estacao STRING,
                                dist FLOAT64,
                                sistema STRING>(
                                a.id, b.id, b.estacao,
                                ST_DISTANCE(a.geom, b.geom),
                                b.sistema
                                )
                                ORDER BY ST_DISTANCE(a.geom, b.geom)
                            ) AS ar
                        FROM (SELECT id, geom FROM centroid_h3) a
                        CROSS JOIN(
                            SELECT id, estacao, sistema, geom
                            FROM estacoes_pluviometricas
                            WHERE geom is not null
                        ) b
                    WHERE a.id <> b.id
                    GROUP BY a.id
                    ) ab
                    CROSS JOIN UNNEST(ab.ar) s
                )

                SELECT
                    *,
                    row_number() OVER (PARTITION BY id_h3 ORDER BY dist) AS ranking
                FROM estacoes_mais_proximas
                ORDER BY id_h3, ranking) h3
                LEFT JOIN last_measurements lm
                    ON lm.id_estacao=h3.id_estacao AND lm.sistema=h3.sistema
            ),

            h3_media AS ( -- calcula média de chuva para as 3 estações mais próximas
            SELECT
                id_h3,
                CAST(sum(p1_15min)/sum(inv_dist) AS DECIMAL) AS chuva_15min,
                STRING_AGG(estacao ORDER BY estacao) estacoes
            FROM h3_chuvas
            -- WHERE ranking < 4
            GROUP BY id_h3
            ),

            final_table AS (
            SELECT
                h3_media.id_h3,
                nome AS bairro,
                estacoes,
                cast(round(h3_media.chuva_15min,2) AS decimal) AS chuva_15min,
            FROM h3_media
            LEFT JOIN `rj-cor.dados_mestres.h3_grid_res8` h3_grid
                ON h3_grid.id=h3_media.id_h3
            LEFT JOIN `rj-cor.dados_mestres.bairro`
                ON ST_CONTAINS(`rj-cor.dados_mestres.bairro`.geometry, ST_CENTROID(h3_grid.geometry))
            )

        SELECT
        final_table.id_h3,
        bairro,
        chuva_15min,
        estacoes,
        CASE
            WHEN chuva_15min> 0     AND chuva_15min<= 1.25 THEN 'chuva fraca'
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
        WITH datas AS (
            (SELECT
                    MAX(
                    DATETIME(
                        CONCAT(data_particao," ", horario)
                    )
                    ) AS last_update
                FROM `rj-cor.clima_pluviometro.taxa_precipitacao_alertario`
                WHERE data_particao> DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 2 DAY))
            UNION ALL
            (SELECT
                    MAX(
                    DATETIME(
                        CONCAT(data_particao," ", horario)
                    )
                    ) AS last_update
                FROM `rj-cor.clima_pluviometro.taxa_precipitacao_websirene`
                WHERE data_particao> DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 2 DAY))
            UNION ALL
            (SELECT
                    MAX(DATETIME(data_medicao)) AS last_update
                FROM `rj-cor.clima_pluviometro.taxa_precipitacao_cemaden`
                WHERE data_particao> DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 2 DAY))
        )
        SELECT
            MAX(last_update) AS last_update
        FROM datas
        """,
    }
