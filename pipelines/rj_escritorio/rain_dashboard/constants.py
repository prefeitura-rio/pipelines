# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Constants for the rain dashboard pipeline
"""
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    RAIN_DASHBOARD_FLOW_NAME = "EMD: Atualizar dados de chuva na api.dados.rio"
    RAIN_DASHBOARD_FLOW_SCHEDULE_PARAMETERS = {
        "query_data": """
        WITH
            max_timestamps AS (
            SELECT
                MAX(data_particao) AS max_data_particao
            FROM `rj-cor.clima_pluviometro.taxa_precipitacao_alertario`
            ),

            alertario AS (
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
            JOIN max_timestamps b
                ON  a.data_particao = b.max_data_particao
            WHERE a.row_num = 1

            -- MAX FROM AFTER A GIVEN DATE
            -- SELECT
            --   id_estacao,
            --   MAX(acumulado_chuva_15_min) AS acumulado_chuva_15_min,
            -- FROM `rj-cor.clima_pluviometro.taxa_precipitacao_alertario` t1
            -- WHERE TRUE
            --     AND DATE_TRUNC(t1.data_particao, day) > '2023-02-07'
            -- GROUP BY id_estacao
            ),
            last_measurements AS (
            SELECT
                a.id_estacao,
                a.data_update,
                "alertario" AS sistema,
                a.acumulado_chuva_15_min,
            FROM alertario a
            ),


            h3_chuvas AS (
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
                    SELECT
                        id_estacao AS id,
                        "alertario" AS sistema,
                        ST_GEOGPOINT(CAST(longitude AS FLOAT64),
                        CAST(latitude AS FLOAT64)) AS geom
                    FROM `rj-cor.clima_pluviometro.estacoes_alertario`
                ),

                estacoes_mais_proximas AS (
                    SELECT AS VALUE s
                    FROM (
                        SELECT
                            ARRAY_AGG(
                                STRUCT<id_h3 STRING,
                                id_estacao STRING,
                                dist FLOAT64,
                                sistema STRING>(
                                a.id, b.id,
                                ST_DISTANCE(a.geom, b.geom),
                                b.sistema
                                )
                                ORDER BY ST_DISTANCE(a.geom, b.geom)
                            ) AS ar
                        FROM (SELECT id, geom FROM centroid_h3) a
                        CROSS JOIN(
                            SELECT id, sistema, geom
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



            h3_media AS (
            SELECT
                id_h3,
                CAST(sum(p1_15min)/sum(inv_dist) AS DECIMAL) AS chuva_15min,
            FROM h3_chuvas
            GROUP BY id_h3
            ),

            final_table AS (
            SELECT
                h3_media.id_h3,
                nome AS bairro,
                cast(round(h3_media.chuva_15min,2) AS decimal) AS chuva_15min,
            FROM h3_media
            LEFT JOIN `rj-cor.dados_mestres.h3_grid_res8` h3_grid
                ON h3_grid.id=h3_media.id_h3
            INNER JOIN `rj-cor.dados_mestres.bairro`
                ON ST_CONTAINS(`rj-cor.dados_mestres.bairro`.geometry, ST_CENTROID(h3_grid.geometry))
            ),

        estacao_por_h3  as (
            SELECT
            h3_grid.id as id_h3,
            STRING_AGG(estacao ORDER BY estacao) estacoes
            FROM `rj-cor.dados_mestres.h3_grid_res8` h3_grid
            INNER JOIN `rj-cor.clima_pluviometro.estacoes_alertario` alertario
                ON ST_CONTAINS(h3_grid.geometry, ST_GEOGPOINT(CAST(alertario.longitude AS FLOAT64), CAST(alertario.latitude AS FLOAT64)))
            GROUP BY h3_grid.id
            )

        SELECT
        final_table.id_h3,
        bairro,
        chuva_15min,
        estacao_por_h3.estacoes,
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
        LEFT JOIN estacao_por_h3 ON final_table.id_h3 = estacao_por_h3.id_h3
        """,
        "query_update": """
        SELECT
            MAX(
            DATETIME(
                CONCAT(data_particao," ", horario)
            )
            ) AS last_update
        FROM `rj-cor.clima_pluviometro.taxa_precipitacao_alertario`
        WHERE data_particao> DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 2 DAY)
        """,
    }
