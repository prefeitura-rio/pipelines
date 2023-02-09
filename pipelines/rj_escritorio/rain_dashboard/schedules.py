# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Schedules for rj_escritorio.rain_dashboard.
"""

from datetime import timedelta

import pendulum
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

every_fifteen_minutes = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=15),
            start_date=pendulum.datetime(2021, 1, 1, 0, 5, 0, tz="America/Sao_Paulo"),
            labels=[
                constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "query": """
            WITH alertario AS (
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
            )
            WHERE row_num = 1
                AND data_particao = CURRENT_DATE('America/Sao_Paulo')
            -- SELECT
            --   id_estacao,
            --   MAX(acumulado_chuva_15_min) AS acumulado_chuva_15_min,
            -- FROM `rj-cor.clima_pluviometro.taxa_precipitacao_alertario` t1
            -- WHERE TRUE
            --     AND DATE_TRUNC(t1.data_particao, day) > '2023-01-09'
            -- GROUP BY id_estacao
            ),

            last_measurements AS (
            SELECT
                a.id_estacao,
                "alertario" AS sistema,
                a.acumulado_chuva_15_min,
            FROM alertario a
            ),

            h3_chuvas AS (
            SELECT
                h3.*,
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
                    ) ab CROSS JOIN
                    UNNEST(ab.ar) s
                )

                SELECT
                *,
                row_number() OVER (PARTITION BY id_h3 ORDER BY dist) AS ranking
                FROM estacoes_mais_proximas
                ORDER BY id_h3, ranking
                ) h3
                LEFT JOIN last_measurements
                lm ON lm.id_estacao=h3.id_estacao AND lm.sistema=h3.sistema
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
            )


            SELECT
            id_h3,
            bairro,
            chuva_15min,
            CASE
                WHEN chuva_15min>0     AND chuva_15min<=1.25 THEN 'Chuva Fraca'
                WHEN chuva_15min>1.25  AND chuva_15min<=6.25 THEN 'Chuva Moderada'
                WHEN chuva_15min>6.25  AND chuva_15min<=12.5 THEN 'Chuva Forte'
                WHEN chuva_15min> 12.5                       THEN 'Chuva Muito Forte'
                ELSE 'SEM CHUVA'
            END AS status,
            CASE
                WHEN chuva_15min>0     AND chuva_15min<=1.25 THEN '#00CCFF'
                WHEN chuva_15min>1.25  AND chuva_15min<=6.25 THEN '#BFA230'
                WHEN chuva_15min>6.25  AND chuva_15min<=12.5 THEN '#E0701F'
                WHEN chuva_15min> 12.5                       THEN '#FF0000'
                ELSE '#ffffff'
            END AS color
            FROM final_table
            """
            },
        )
    ]
)
