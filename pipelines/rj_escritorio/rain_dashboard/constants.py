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
            alertario AS ( -- seleciona a última medição do alertario de cada estação nos últimos 30min
            SELECT
                id_estacao,
                CAST(acumulado_chuva AS FLOAT64) acumulado_chuva,
                CURRENT_DATE('America/Sao_Paulo') as data,
                data_update
            FROM (
                SELECT
                id_estacao,
                acumulado_chuva_15min AS acumulado_chuva,
                DATETIME(data_medicao) AS data_update,
                ROW_NUMBER() OVER (
                    PARTITION BY id_estacao ORDER BY DATETIME(data_medicao) DESC
                ) AS row_num
                FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
                WHERE -- data_medicao >= CAST(TIME_SUB(CURRENT_TIME('America/Sao_Paulo'), INTERVAL 30 MINUTE) AS STRING)
                -- AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
                    data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 4 DAY) AS STRING)
            )AS a
            WHERE a.row_num = 1
            ),

            -- websirene AS ( -- seleciona a última medição do websirene de cada estação nos últimos 30min
            -- SELECT
            --     id_estacao,
            --     acumulado_chuva_15_min,
            --     CURRENT_DATE('America/Sao_Paulo') as data,
            --     data_update
            -- FROM (
            --     SELECT
            --     id_estacao,
            --     acumulado_chuva_15_min,
            --     data_particao,
            --     DATETIME(CONCAT(data_particao," ", horario)) AS data_update,
            --     ROW_NUMBER() OVER (
            --         PARTITION BY id_estacao ORDER BY DATETIME(CONCAT(data_particao," ", horario)) DESC
            --     ) AS row_num
            --     FROM `rj-cor.clima_pluviometro.taxa_precipitacao_websirene`
            --     WHERE data_particao> DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY)
            --         AND horario >= TIME_SUB(CURRENT_TIME('America/Sao_Paulo'), INTERVAL 30 MINUTE)
            -- )AS a
            -- WHERE a.row_num = 1
            -- ),

            -- cemaden AS ( -- seleciona a última medição do cemaden de cada estação nos últimos 30min
            -- SELECT
            --     id_estacao,
            --     acumulado_chuva_10_min AS acumulado_chuva_15_min,
            --     CURRENT_DATE('America/Sao_Paulo') as data,
            --     data_update
            -- FROM (
            --     SELECT
            --     id_estacao,
            --     acumulado_chuva_10_min,
            --     data_particao,
            --     DATETIME(data_medicao) AS data_update,
            --     ROW_NUMBER() OVER (
            --         PARTITION BY id_estacao ORDER BY DATETIME(data_medicao) DESC
            --     ) AS row_num
            --     FROM `rj-cor.clima_pluviometro.taxa_precipitacao_cemaden`
            --     WHERE data_particao> DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY)
            --             AND data_medicao >= TIMESTAMP_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 30 MINUTE)
            -- )AS a
            -- WHERE a.row_num = 1
            -- ),

            last_measurements AS (-- concatena medições do alertario, cemaden e websirene
            (SELECT
                id_estacao,
                data_update,
                "alertario" AS sistema,
                acumulado_chuva,
            FROM alertario
            WHERE acumulado_chuva IS NOT NULL)
            -- UNION ALL
            -- (SELECT
            --     id_estacao,
            --     data_update,
            --     "websirene" AS sistema,
            --     acumulado_chuva_15_min,
            -- FROM websirene)
            -- UNION ALL
            -- (SELECT
            --     id_estacao,
            --     data_update,
            --     "cemaden" AS sistema,
            --     acumulado_chuva_15_min,
            -- FROM cemaden)
            ),

            -- choosing the neighborhood that shares the most intersection with the given H3 ID
            intersected_areas AS (
              SELECT
                h3_grid.id,
                bairros.nome AS bairro,
                ST_CENTROID(h3_grid.geometry) AS geom,
                -- h3_grid.geometry,
                -- bairros.geometry,
                ST_AREA(ST_INTERSECTION(bairros.geometry, h3_grid.geometry)) AS intersection_area,
                ROW_NUMBER() OVER (PARTITION BY h3_grid.id ORDER BY ST_AREA(ST_INTERSECTION(bairros.geometry, h3_grid.geometry)) DESC) AS row_num
              FROM
                `rj-cor.dados_mestres.h3_grid_res8` h3_grid
              LEFT JOIN
                `rj-cor.dados_mestres.bairro` AS bairros
              ON
                ST_INTERSECTS(bairros.geometry, h3_grid.geometry)
              WHERE NOT ST_CONTAINS(ST_GEOGFROMTEXT('POLYGON((-43.35167114973923 -23.03719187431942, -43.21742224531541 -23.11411703410819, -43.05787930227852 -23.08560586153892, -43.13797293161925 -22.9854505090441, -43.24908435505957 -23.01309491285712, -43.29357259322761 -23.02302500142027, -43.35372293867113 -23.02286949608791, -43.35167114973923 -23.03719187431942))'), h3_grid.geometry)
                AND NOT ST_CONTAINS(ST_GEOGFROMTEXT('POLYGON((-43.17255470033881 -22.80357287766821, -43.16164114820394 -22.8246787848653, -43.1435175784006 -22.83820699694974, -43.08831858222521 -22.79901386772875, -43.09812065965735 -22.76990583135868, -43.11917632397501 -22.77502040608505, -43.12252626904735 -22.74275730775724, -43.13510053525226 -22.7443347361711, -43.1568784870256 -22.79110122556994, -43.17255470033881 -22.80357287766821))'), h3_grid.geometry)
                AND h3_grid.id not in ("88a8a06a31fffff", "88a8a069b5fffff", "88a8a3d357fffff", "88a8a3d355fffff", "88a8a068adfffff", "88a8a06991fffff", "88a8a06999fffff")
            ),

            h3_chuvas AS ( -- calcula qnt de chuva para cada h3
            SELECT
                h3.*,
                lm.id_estacao,
                lm.acumulado_chuva,
                lm.acumulado_chuva/power(h3.dist,5) AS p1_15min,
                1/power(h3.dist,5) AS inv_dist
            FROM (
                WITH centroid_h3 AS (
                    SELECT
                        *
                    FROM intersected_areas
                    WHERE row_num = 1
                ),

                estacoes_pluviometricas AS (
                    (SELECT
                        id_estacao AS id,
                        estacao,
                        "alertario" AS sistema,
                        ST_GEOGPOINT(CAST(longitude AS FLOAT64),
                        CAST(latitude AS FLOAT64)) AS geom
                    FROM `rj-cor.clima_pluviometro.estacoes_alertario`)
                    -- UNION ALL
                    -- (SELECT
                    --     id_estacao AS id,
                    --     estacao,
                    --     "websirene" AS sistema,
                    --     ST_GEOGPOINT(CAST(longitude AS FLOAT64),
                    --     CAST(latitude AS FLOAT64)) AS geom
                    -- FROM `rj-cor.clima_pluviometro.estacoes_websirene`)
                    -- UNION ALL
                    -- (SELECT
                    --     id_estacao AS id,
                    --     estacao,
                    --     "cemaden" AS sistema,
                    --     ST_GEOGPOINT(CAST(longitude AS FLOAT64),
                    --     CAST(latitude AS FLOAT64)) AS geom
                    -- FROM `rj-cor.clima_pluviometro.estacoes_cemaden`)
                ),

                estacoes_mais_proximas AS (
                    SELECT AS VALUE s
                    FROM (
                        SELECT
                            ARRAY_AGG(
                                STRUCT<id_h3 STRING,
                                id_estacao STRING,
                                estacao STRING,
                                bairro STRING,
                                dist FLOAT64,
                                sistema STRING>(
                                    a.id, b.id, b.estacao, a.bairro,
                                    ST_DISTANCE(a.geom, b.geom),
                                    b.sistema
                                )
                                ORDER BY ST_DISTANCE(a.geom, b.geom)
                            ) AS ar
                        FROM (SELECT id, geom, bairro FROM centroid_h3) a
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

            final_table AS ( -- calcula média de chuva para as 3 estações mais próximas
            SELECT
                id_h3,
                bairro,
                cast(round(CAST(sum(p1_15min)/sum(inv_dist) AS DECIMAL),2) AS decimal) AS qnt_chuva,
                STRING_AGG(estacao ORDER BY estacao) estacoes
            FROM h3_chuvas
            -- WHERE ranking < 4
            GROUP BY id_h3, bairro
            )

        SELECT
        final_table.id_h3,
        bairro,
        COALESCE(qnt_chuva, 0) chuva_15min,
        COALESCE(qnt_chuva, 0) quantidade,
        estacoes,
        CASE
            WHEN qnt_chuva > 0     AND qnt_chuva <= 1.25 THEN 'chuva fraca'
            WHEN qnt_chuva > 1.25  AND qnt_chuva <= 6.25 THEN 'chuva moderada'
            WHEN qnt_chuva > 6.25  AND qnt_chuva <= 12.5 THEN 'chuva forte'
            WHEN qnt_chuva > 12.5                         THEN 'chuva muito forte'
            ELSE 'sem chuva'
        END AS status,
        CASE
            WHEN qnt_chuva > 0     AND qnt_chuva <= 1.25 THEN '#DAECFB'
            WHEN qnt_chuva > 1.25  AND qnt_chuva <= 6.25 THEN '#A9CBE8'
            WHEN qnt_chuva > 6.25  AND qnt_chuva <= 12.5 THEN '#77A9D5'
            WHEN qnt_chuva > 12.5                         THEN '#125999'
            ELSE '#ffffff'
        END AS color
        FROM final_table
        """,
        "query_update": """
        WITH datas AS (
            (
              SELECT
                MAX(
                  DATETIME(data_medicao)
                ) AS last_update
              FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
              WHERE -- data_medicao >= CAST(TIME_SUB(CURRENT_TIME('America/Sao_Paulo'), INTERVAL 30 MINUTE) AS STRING)
                -- AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
                    data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 4 DAY) AS STRING)
            )
            -- UNION ALL
            -- (
            --   SELECT
            --     MAX(
            --       DATETIME(
            --           CONCAT(data_particao," ", horario)
            --       )
            --     ) AS last_update
            --   FROM `rj-cor.clima_pluviometro.taxa_precipitacao_websirene`
            --   WHERE data_particao> DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 2 DAY)
            --     AND CAST(CONCAT(data_particao, " ", horario) AS DATETIME) <= CURRENT_DATETIME('America/Sao_Paulo')
            -- )
            -- UNION ALL
            -- (
            --   SELECT
            --     MAX(DATETIME(data_medicao)) AS last_update
            --   FROM `rj-cor.clima_pluviometro.taxa_precipitacao_cemaden`
            --   WHERE data_particao> DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 2 DAY)
            --     AND data_medicao <= CURRENT_DATETIME('America/Sao_Paulo')
            -- )
        )
        SELECT
            MAX(last_update) AS last_update
        FROM datas
        """,
    }
