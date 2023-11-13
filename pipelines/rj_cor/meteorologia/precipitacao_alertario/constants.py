# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Constant values for the rj_cor.meteorologia.precipitacao_alertario project
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the precipitacao_alertario project
    """

    RAIN_DASHBOARD_LAST_2H_FLOW_SCHEDULE_PARAMETERS = {
        "redis_data_key": "data_chuva_passado_alertario",
        "redis_update_key": "data_update_chuva_passado_alertario",
        "query_data": """
        WITH
            last_update_date AS (
            SELECT
                CAST(MAX(data_particao) AS DATETIME) AS last_update
            FROM `rj-cor.clima_pluviometro.taxa_precipitacao_alertario`
            WHERE data_particao >= DATE_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 2 DAY)
        ),
            alertario AS ( -- seleciona as últimas 2h de medição antes da última atualização
            SELECT
                id_estacao,
                acumulado_chuva_15_min,
                CURRENT_DATE('America/Sao_Paulo') as data,
                data_particao,
                DATETIME(CONCAT(data_particao," ", horario)) AS data_update,
                FROM `rj-cor.clima_pluviometro.taxa_precipitacao_alertario`
                INNER JOIN last_update_date lup ON 1=1
                WHERE data_particao >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 2 DAY)
                  AND CAST(CONCAT(data_particao, " ", horario) AS DATETIME) >= DATE_SUB(lup.last_update, INTERVAL 2 HOUR)
            ),

            last_measurements AS (-- soma a quantidade chuva das últimas 2h
            SELECT
                a.id_estacao,
                "alertario" AS sistema,
                MAX(a.data_update) AS data_update,
                SUM(a.acumulado_chuva_15_min) AS acumulado_chuva_15_min,
            FROM alertario a
            GROUP BY a.id_estacao, sistema
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
                    SELECT
                        id_estacao AS id,
                        estacao,
                        "alertario" AS sistema,
                        ST_GEOGPOINT(CAST(longitude AS FLOAT64),
                        CAST(latitude AS FLOAT64)) AS geom
                    FROM `rj-cor.clima_pluviometro.estacoes_alertario`
                ),

                estacoes_mais_proximas AS ( -- calcula distância das estações para cada centróide do h3
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
                h3_media.estacoes,
                nome AS bairro,
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
            MAX(
            DATETIME(
                CONCAT(data_particao," ", horario)
            )
            ) AS last_update
        FROM `rj-cor.clima_pluviometro.taxa_precipitacao_alertario`
        WHERE data_particao> DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 2 DAY)
        """,
    }
