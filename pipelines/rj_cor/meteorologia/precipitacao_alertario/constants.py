# -*- coding: utf-8 -*-
# flake8: noqa: E501
# pylint: disable=C0302
"""
Constant values for the rj_cor.meteorologia.precipitacao_alertario project
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the precipitacao_alertario project
    The constants for actual values are on rain_dashboard_constants file
    """

    DATASET_ID_PLUVIOMETRIC = "clima_pluviometro"
    TABLE_ID_PLUVIOMETRIC = "taxa_precipitacao_alertario_5min"
    TABLE_ID_PLUVIOMETRIC_OLD_API = "taxa_precipitacao_alertario"
    DATASET_ID_METEOROLOGICAL = "clima_estacao_meteorologica"
    TABLE_ID_METEOROLOGICAL = "meteorologia_alertario"

    RAIN_DASHBOARD_LAST_30MIN_FLOW_SCHEDULE_PARAMETERS = {
        "redis_data_key": "data_last_30min_rain",
        "redis_update_key": "data_last_30min_rain_update",
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
                acumulado_chuva_30min acumulado_chuva,
                DATETIME(data_medicao) AS data_update,
                ROW_NUMBER() OVER (
                    PARTITION BY id_estacao ORDER BY DATETIME(data_medicao) DESC
                ) AS row_num
                FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
                WHERE data_medicao >= CAST(TIME_SUB(CURRENT_TIME('America/Sao_Paulo'), INTERVAL 30 MINUTE) AS STRING)
                    AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
            )AS a
            WHERE a.row_num = 1
            ),

            last_measurements AS (
            (SELECT
                id_estacao,
                data_update,
                "alertario" AS sistema,
                acumulado_chuva,
            FROM alertario)
            ),

            -- choosing the neighborhood that shares the most intersection with the given H3 ID
            intersected_areas AS (
              SELECT
                h3_grid.id,
                bairros.nome AS bairro,
                ST_CENTROID(h3_grid.geometry) AS geom,
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
            GROUP BY id_h3, bairro
            )

        SELECT
        final_table.id_h3,
        bairro,
        qnt_chuva quantidade,
        estacoes,
        CASE
            WHEN qnt_chuva > 2*0     AND qnt_chuva <= 2*1.25 THEN 'chuva fraca'
            WHEN qnt_chuva > 2*1.25  AND qnt_chuva <= 2*6.25 THEN 'chuva moderada'
            WHEN qnt_chuva > 2*6.25  AND qnt_chuva <= 2*12.5 THEN 'chuva forte'
            WHEN qnt_chuva > 2*12.5                          THEN 'chuva muito forte'
            ELSE 'sem chuva'
        END AS status,
        CASE
            WHEN qnt_chuva > 2*0     AND qnt_chuva <= 2*1.25 THEN '#DAECFB'
            WHEN qnt_chuva > 2*1.25  AND qnt_chuva <= 2*6.25 THEN '#A9CBE8'
            WHEN qnt_chuva > 2*6.25  AND qnt_chuva <= 2*12.5 THEN '#77A9D5'
            WHEN qnt_chuva > 2*12.5                          THEN '#125999'
            ELSE '#ffffff'
        END AS color
        FROM final_table
        """,
        "query_update": """
        SELECT
            MAX(
            DATETIME(data_medicao)
            ) AS last_update
        FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
        WHERE data_medicao > CAST(DATE_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
            AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
        """,
    }

    RAIN_DASHBOARD_LAST_60MIN_FLOW_SCHEDULE_PARAMETERS = {
        "redis_data_key": "data_last_60min_rain",
        "redis_update_key": "data_last_60min_rain_update",
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
                acumulado_chuva_1h acumulado_chuva,
                DATETIME(data_medicao) AS data_update,
                ROW_NUMBER() OVER (
                    PARTITION BY id_estacao ORDER BY DATETIME(data_medicao) DESC
                ) AS row_num
                FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
                WHERE data_medicao >= CAST(TIME_SUB(CURRENT_TIME('America/Sao_Paulo'), INTERVAL 30 MINUTE) AS STRING)
                    AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
            )AS a
            WHERE a.row_num = 1
            ),

            last_measurements AS (
            (SELECT
                id_estacao,
                data_update,
                "alertario" AS sistema,
                acumulado_chuva,
            FROM alertario)
            ),

            -- choosing the neighborhood that shares the most intersection with the given H3 ID
            intersected_areas AS (
              SELECT
                h3_grid.id,
                bairros.nome AS bairro,
                ST_CENTROID(h3_grid.geometry) AS geom,
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
            GROUP BY id_h3, bairro
            )

        SELECT
        final_table.id_h3,
        bairro,
        qnt_chuva quantidade,
        estacoes,
        CASE
            WHEN qnt_chuva > 4*0     AND qnt_chuva <= 4*1.25 THEN 'chuva fraca'
            WHEN qnt_chuva > 4*1.25  AND qnt_chuva <= 4*6.25 THEN 'chuva moderada'
            WHEN qnt_chuva > 4*6.25  AND qnt_chuva <= 4*12.5 THEN 'chuva forte'
            WHEN qnt_chuva > 4*12.5                          THEN 'chuva muito forte'
            ELSE 'sem chuva'
        END AS status,
        CASE
            WHEN qnt_chuva > 4*0     AND qnt_chuva <= 4*1.25 THEN '#DAECFB'
            WHEN qnt_chuva > 4*1.25  AND qnt_chuva <= 4*6.25 THEN '#A9CBE8'
            WHEN qnt_chuva > 4*6.25  AND qnt_chuva <= 4*12.5 THEN '#77A9D5'
            WHEN qnt_chuva > 4*12.5                          THEN '#125999'
            ELSE '#ffffff'
        END AS color
        FROM final_table
        """,
        "query_update": """
        SELECT
            MAX(
            DATETIME(data_medicao)
            ) AS last_update
        FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
        WHERE data_medicao > CAST(DATE_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
            AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
        """,
    }

    RAIN_DASHBOARD_LAST_2H_FLOW_SCHEDULE_PARAMETERS = {
        "redis_data_key": "data_last_120min_rain",
        "redis_update_key": "data_last_120min_rain_update",
        "query_data": """rec
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
                acumulado_chuva_2h acumulado_chuva,
                DATETIME(data_medicao) AS data_update,
                ROW_NUMBER() OVER (
                    PARTITION BY id_estacao ORDER BY DATETIME(data_medicao) DESC
                ) AS row_num
                FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
                WHERE data_medireccao >= CAST(TIME_SUB(CURRENT_TIME('America/Sao_Paulo'), INTERVAL 30 MINUTE) AS STRING)
                    AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
            )AS a
            WHERE a.row_num = 1
            ),

            last_measurements AS (
            (SELECT
                id_estacao,
                data_update,
                "alertario" AS sistema,
                acumulado_chuva,
            FROM alertario)
            ),

            -- choosing the neighborhood that shares the most intersection with the given H3 ID
            intersected_areas AS (
              SELECT
                h3_grid.id,
                bairros.nome AS bairro,
                ST_CENTROID(h3_grid.geometry) AS geom,
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
            GROUP BY id_h3, bairro
            )

        SELECT
        final_table.id_h3,
        bairro,
        qnt_chuva quantidade,
        qnt_chuva chuva_15min,
        estacoes,
        CASE
            WHEN qnt_chuva > 8*0     AND qnt_chuva <= 8*1.25 THEN 'chuva fraca'
            WHEN qnt_chuva > 8*1.25  AND qnt_chuva <= 8*6.25 THEN 'chuva moderada'
            WHEN qnt_chuva > 8*6.25  AND qnt_chuva <= 8*12.5 THEN 'chuva forte'
            WHEN qnt_chuva > 8*12.5                         THEN 'chuva muito forte'
            ELSE 'sem chuva'
        END AS status,
        CASE
            WHEN qnt_chuva > 8*0     AND qnt_chuva <= 8*1.25 THEN '#DAECFB'
            WHEN qnt_chuva > 8*1.25  AND qnt_chuva <= 8*6.25 THEN '#A9CBE8'
            WHEN qnt_chuva > 8*6.25  AND qnt_chuva <= 8*12.5 THEN '#77A9D5'
            WHEN qnt_chuva > 8*12.5                         THEN '#125999'
            ELSE '#ffffff'
        END AS color
        FROM final_table
        """,
        "query_update": """
        SELECT
            MAX(
            DATETIME(data_medicao)
            ) AS last_update
        FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
        WHERE data_medicao > CAST(DATE_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
            AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
        """,
    }

    RAIN_DASHBOARD_LAST_3H_FLOW_SCHEDULE_PARAMETERS = {
        "redis_data_key": "data_last_3h_rain",
        "redis_update_key": "data_last_3h_rain_update",
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
                acumulado_chuva_3h acumulado_chuva,
                DATETIME(data_medicao) AS data_update,
                ROW_NUMBER() OVER (
                    PARTITION BY id_estacao ORDER BY DATETIME(data_medicao) DESC
                ) AS row_num
                FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
                WHERE data_medicao >= CAST(TIME_SUB(CURRENT_TIME('America/Sao_Paulo'), INTERVAL 30 MINUTE) AS STRING)
                    AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
            )AS a
            WHERE a.row_num = 1
            ),

            last_measurements AS (
            (SELECT
                id_estacao,
                data_update,
                "alertario" AS sistema,
                acumulado_chuva,
            FROM alertario)
            ),

            -- choosing the neighborhood that shares the most intersection with the given H3 ID
            intersected_areas AS (
              SELECT
                h3_grid.id,
                bairros.nome AS bairro,
                ST_CENTROID(h3_grid.geometry) AS geom,
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
            GROUP BY id_h3, bairro
            )

        SELECT
        final_table.id_h3,
        bairro,
        qnt_chuva quantidade,
        estacoes,
        CASE
            WHEN qnt_chuva > 12*0     AND qnt_chuva <= 12*1.25 THEN 'chuva fraca'
            WHEN qnt_chuva > 12*1.25  AND qnt_chuva <= 12*6.25 THEN 'chuva moderada'
            WHEN qnt_chuva > 12*6.25  AND qnt_chuva <= 12*12.5 THEN 'chuva forte'
            WHEN qnt_chuva > 12*12.5                         THEN 'chuva muito forte'
            ELSE 'sem chuva'
        END AS status,
        CASE
            WHEN qnt_chuva > 12*0     AND qnt_chuva <= 12*1.25 THEN '#DAECFB'
            WHEN qnt_chuva > 12*1.25  AND qnt_chuva <= 12*6.25 THEN '#A9CBE8'
            WHEN qnt_chuva > 12*6.25  AND qnt_chuva <= 12*12.5 THEN '#77A9D5'
            WHEN qnt_chuva > 12*12.5                         THEN '#125999'
            ELSE '#ffffff'
        END AS color
        FROM final_table
        """,
        "query_update": """
        SELECT
            MAX(
            DATETIME(data_medicao)
            ) AS last_update
        FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
        WHERE data_medicao > CAST(DATE_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
            AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
        """,
    }

    RAIN_DASHBOARD_LAST_6H_FLOW_SCHEDULE_PARAMETERS = {
        "redis_data_key": "data_last_6h_rain",
        "redis_update_key": "data_last_6h_rain_update",
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
                acumulado_chuva_6h acumulado_chuva,
                DATETIME(data_medicao) AS data_update,
                ROW_NUMBER() OVER (
                    PARTITION BY id_estacao ORDER BY DATETIME(data_medicao) DESC
                ) AS row_num
                FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
                WHERE data_medicao >= CAST(TIME_SUB(CURRENT_TIME('America/Sao_Paulo'), INTERVAL 30 MINUTE) AS STRING)
                    AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
            )AS a
            WHERE a.row_num = 1
            ),

            last_measurements AS (
            (SELECT
                id_estacao,
                data_update,
                "alertario" AS sistema,
                acumulado_chuva,
            FROM alertario)
            ),

            -- choosing the neighborhood that shares the most intersection with the given H3 ID
            intersected_areas AS (
              SELECT
                h3_grid.id,
                bairros.nome AS bairro,
                ST_CENTROID(h3_grid.geometry) AS geom,
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
            GROUP BY id_h3, bairro
            )

        SELECT
        final_table.id_h3,
        bairro,
        qnt_chuva quantidade,
        estacoes,
        CASE
            WHEN qnt_chuva > 24*0     AND qnt_chuva <= 24*1.25 THEN 'chuva fraca'
            WHEN qnt_chuva > 24*1.25  AND qnt_chuva <= 24*6.25 THEN 'chuva moderada'
            WHEN qnt_chuva > 24*6.25  AND qnt_chuva <= 24*12.5 THEN 'chuva forte'
            WHEN qnt_chuva > 24*12.5                         THEN 'chuva muito forte'
            ELSE 'sem chuva'
        END AS status,
        CASE
            WHEN qnt_chuva > 24*0     AND qnt_chuva <= 24*1.25 THEN '#DAECFB'
            WHEN qnt_chuva > 24*1.25  AND qnt_chuva <= 24*6.25 THEN '#A9CBE8'
            WHEN qnt_chuva > 24*6.25  AND qnt_chuva <= 24*12.5 THEN '#77A9D5'
            WHEN qnt_chuva > 24*12.5                         THEN '#125999'
            ELSE '#ffffff'
        END AS color
        FROM final_table
        """,
        "query_update": """
        SELECT
            MAX(
            DATETIME(data_medicao)
            ) AS last_update
        FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
        WHERE data_medicao > CAST(DATE_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
            AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
        """,
    }

    RAIN_DASHBOARD_LAST_12H_FLOW_SCHEDULE_PARAMETERS = {
        "redis_data_key": "data_last_12h_rain",
        "redis_update_key": "data_last_12h_rain_update",
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
                acumulado_chuva_12h acumulado_chuva,
                DATETIME(data_medicao) AS data_update,
                ROW_NUMBER() OVER (
                    PARTITION BY id_estacao ORDER BY DATETIME(data_medicao) DESC
                ) AS row_num
                FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
                WHERE data_medicao >= CAST(TIME_SUB(CURRENT_TIME('America/Sao_Paulo'), INTERVAL 30 MINUTE) AS STRING)
                    AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
            )AS a
            WHERE a.row_num = 1
            ),

            last_measurements AS (
            (SELECT
                id_estacao,
                data_update,
                "alertario" AS sistema,
                acumulado_chuva,
            FROM alertario)
            ),

            -- choosing the neighborhood that shares the most intersection with the given H3 ID
            intersected_areas AS (
              SELECT
                h3_grid.id,
                bairros.nome AS bairro,
                ST_CENTROID(h3_grid.geometry) AS geom,
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
            GROUP BY id_h3, bairro
            )

        SELECT
        final_table.id_h3,
        bairro,
        qnt_chuva quantidade,
        estacoes,
        CASE
            WHEN qnt_chuva > 48*0     AND qnt_chuva <= 48*1.25 THEN 'chuva fraca'
            WHEN qnt_chuva > 48*1.25  AND qnt_chuva <= 48*6.25 THEN 'chuva moderada'
            WHEN qnt_chuva > 48*6.25  AND qnt_chuva <= 48*12.5 THEN 'chuva forte'
            WHEN qnt_chuva > 48*12.5                           THEN 'chuva muito forte'
            ELSE 'sem chuva'
        END AS status,
        CASE
            WHEN qnt_chuva > 48*0     AND qnt_chuva <= 48*1.25 THEN '#DAECFB'
            WHEN qnt_chuva > 48*1.25  AND qnt_chuva <= 48*6.25 THEN '#A9CBE8'
            WHEN qnt_chuva > 48*6.25  AND qnt_chuva <= 48*12.5 THEN '#77A9D5'
            WHEN qnt_chuva > 48*12.5                           THEN '#125999'
            ELSE '#ffffff'
        END AS color
        FROM final_table
        """,
        "query_update": """
        SELECT
            MAX(
            DATETIME(data_medicao)
            ) AS last_update
        FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
        WHERE data_medicao > CAST(DATE_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
            AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
        """,
    }

    RAIN_DASHBOARD_LAST_24H_FLOW_SCHEDULE_PARAMETERS = {
        "redis_data_key": "data_last_24h_rain",
        "redis_update_key": "data_last_24h_rain_update",
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
                acumulado_chuva_24h acumulado_chuva,
                DATETIME(data_medicao) AS data_update,
                ROW_NUMBER() OVER (
                    PARTITION BY id_estacao ORDER BY DATETIME(data_medicao) DESC
                ) AS row_num
                FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
                WHERE data_medicao >= CAST(TIME_SUB(CURRENT_TIME('America/Sao_Paulo'), INTERVAL 30 MINUTE) AS STRING)
                    AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
            )AS a
            WHERE a.row_num = 1
            ),

            last_measurements AS (
            (SELECT
                id_estacao,
                data_update,
                "alertario" AS sistema,
                acumulado_chuva,
            FROM alertario)
            ),

            -- choosing the neighborhood that shares the most intersection with the given H3 ID
            intersected_areas AS (
              SELECT
                h3_grid.id,
                bairros.nome AS bairro,
                ST_CENTROID(h3_grid.geometry) AS geom,
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
            GROUP BY id_h3, bairro
            )

        SELECT
        final_table.id_h3,
        bairro,
        qnt_chuva quantidade,
        estacoes,
        CASE
            WHEN qnt_chuva > 24*4*0     AND qnt_chuva <= 24*4*1.25 THEN 'chuva fraca'
            WHEN qnt_chuva > 24*4*1.25  AND qnt_chuva <= 24*4*6.25 THEN 'chuva moderada'
            WHEN qnt_chuva > 24*4*6.25  AND qnt_chuva <= 24*4*12.5 THEN 'chuva forte'
            WHEN qnt_chuva > 24*4*12.5                             THEN 'chuva muito forte'
            ELSE 'sem chuva'
        END AS status,
        CASE
            WHEN qnt_chuva > 24*4*0     AND qnt_chuva <= 24*4*1.25 THEN '#DAECFB'
            WHEN qnt_chuva > 24*4*1.25  AND qnt_chuva <= 24*4*6.25 THEN '#A9CBE8'
            WHEN qnt_chuva > 24*4*6.25  AND qnt_chuva <= 24*4*12.5 THEN '#77A9D5'
            WHEN qnt_chuva > 24*4*12.5                             THEN '#125999'
            ELSE '#ffffff'
        END AS color
        FROM final_table
        """,
        "query_update": """
        SELECT
            MAX(
            DATETIME(data_medicao)
            ) AS last_update
        FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
        WHERE data_medicao > CAST(DATE_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
            AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
        """,
    }

    RAIN_DASHBOARD_LAST_96H_FLOW_SCHEDULE_PARAMETERS = {
        "redis_data_key": "data_last_96h_rain",
        "redis_update_key": "data_last_96h_rain_update",
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
                acumulado_chuva_96h acumulado_chuva,
                DATETIME(data_medicao) AS data_update,
                ROW_NUMBER() OVER (
                    PARTITION BY id_estacao ORDER BY DATETIME(data_medicao) DESC
                ) AS row_num
                FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
                WHERE data_medicao >= CAST(TIME_SUB(CURRENT_TIME('America/Sao_Paulo'), INTERVAL 30 MINUTE) AS STRING)
                    AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
            )AS a
            WHERE a.row_num = 1
            ),

            last_measurements AS (
            (SELECT
                id_estacao,
                data_update,
                "alertario" AS sistema,
                acumulado_chuva,
            FROM alertario)
            ),

            -- choosing the neighborhood that shares the most intersection with the given H3 ID
            intersected_areas AS (
              SELECT
                h3_grid.id,
                bairros.nome AS bairro,
                ST_CENTROID(h3_grid.geometry) AS geom,
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
            GROUP BY id_h3, bairro
            )

        SELECT
        final_table.id_h3,
        bairro,
        qnt_chuva quantidade,
        estacoes,
        CASE
            WHEN qnt_chuva > 4*96*0     AND qnt_chuva <= 4*96*1.25 THEN 'chuva fraca'
            WHEN qnt_chuva > 4*96*1.25  AND qnt_chuva <= 4*96*6.25 THEN 'chuva moderada'
            WHEN qnt_chuva > 4*96*6.25  AND qnt_chuva <= 4*96*12.5 THEN 'chuva forte'
            WHEN qnt_chuva > 4*96*12.5                             THEN 'chuva muito forte'
            ELSE 'sem chuva'
        END AS status,
        CASE
            WHEN qnt_chuva > 4*96*0     AND qnt_chuva <= 4*96*1.25 THEN '#DAECFB'
            WHEN qnt_chuva > 4*96*1.25  AND qnt_chuva <= 4*96*6.25 THEN '#A9CBE8'
            WHEN qnt_chuva > 4*96*6.25  AND qnt_chuva <= 4*96*12.5 THEN '#77A9D5'
            WHEN qnt_chuva > 4*96*12.5                             THEN '#125999'
            ELSE '#ffffff'
        END AS color
        FROM final_table
        """,
        "query_update": """
        SELECT
            MAX(
            DATETIME(data_medicao)
            ) AS last_update
        FROM `rj-cor.clima_pluviometro_staging.taxa_precipitacao_alertario_5min`
        WHERE data_medicao > CAST(DATE_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
            AND data_particao >= CAST(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY) AS STRING)
        """,
    }
