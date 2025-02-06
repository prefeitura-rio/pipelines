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
    REDIS_NAME = "last_update"
    TABLE_ID_ATIVIDADES_EVENTOS = "ocorrencias_orgaos_responsaveis_nova_api"
    TABLE_ID_POPS = "procedimento_operacional_padrao_nova_api"
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
                CAST(latitude AS FLOAT64)) AS geometry,
                status AS status_ocorrencia,
                data_inicio,
                data_fim,
                row_number() OVER (PARTITION BY id_evento ORDER BY created_at DESC) AS last_update
            -- FROM `rj-cor.adm_cor_comando_staging.ocorrencias`
            FROM `rj-cor.adm_cor_comando_staging.ocorrencias_nova_api`
            WHERE id_pop IN ("5", "6", "31", "32", "33")
                AND CAST(data_particao AS DATE) >= CAST(DATE_TRUNC(TIMESTAMP_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 15 MINUTE), day) AS date)
                -- AND data_fim IS NULL # data_fim não está confiável, temos status fechados sem esse campo
            ),

            intersected_areas AS (
              SELECT
                h3_grid.id AS id_h3,
                bairros.nome AS bairro,
                h3_grid.geometry,
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

            final_table AS (
            SELECT
                id_h3,
                bairro,
                COALESCE(MAX(tipo), 0) AS tipo
            FROM intersected_areas
            LEFT JOIN alagamentos
                ON ST_CONTAINS(intersected_areas.geometry, alagamentos.geometry)
                AND alagamentos.last_update = 1
                -- AND CAST(data_inicio AS DATETIME) >= TIMESTAMP_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 15 MINUTE) -- seleciona ocorrencias que iniciaram nos últimos 15min
                AND (CAST(data_inicio AS DATETIME) >= TIMESTAMP_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 15 MINUTE)
                  OR CAST(data_fim AS DATETIME) >= TIMESTAMP_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 15 MINUTE)
                  OR status_ocorrencia = "Aberto") -- seleciona ocorrencias que iniciaram ou finalizaram nos últimos 15min ou ainda não finalizaram
            WHERE  intersected_areas.row_num = 1
            GROUP BY id_h3, bairro
            )

        SELECT
            id_h3,
            bairro,
            -- tipo AS qnt_alagamentos,
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
            END AS color,
        FROM final_table
        -- order by qnt_alagamentos
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
                CAST(latitude AS FLOAT64)) AS geometry,
                status AS status_ocorrencia,
                data_inicio,
                data_fim,
                row_number() OVER (PARTITION BY id_evento ORDER BY created_at DESC) AS last_update
            -- FROM `rj-cor.adm_cor_comando_staging.ocorrencias`
            FROM `rj-cor.adm_cor_comando_staging.ocorrencias_nova_api`
            WHERE id_pop IN ("5", "6", "31", "32", "33")
            AND CAST(data_particao AS DATE) >= CAST(DATE_TRUNC(TIMESTAMP_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 2 HOUR), day) AS date)
            ),

            intersected_areas AS (
              SELECT
                h3_grid.id AS id_h3,
                bairros.nome AS bairro,
                h3_grid.geometry,
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

            final_table AS (
            SELECT
                id_h3,
                bairro,
                COALESCE(MAX(tipo), 0) AS tipo
            FROM intersected_areas
            LEFT JOIN alagamentos
                ON ST_CONTAINS(intersected_areas.geometry, alagamentos.geometry)
                AND alagamentos.last_update = 1
                AND (CAST(data_inicio AS DATETIME) >= TIMESTAMP_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 2 HOUR)
                  OR CAST(data_fim AS DATETIME) >= TIMESTAMP_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 2 HOUR)
                  OR status_ocorrencia = "Aberto") -- seleciona ocorrencias que iniciaram ou finalizaram nas últimas 2h ou ainda não finalizaram
                -- AND (CAST(data_fim AS DATETIME) >= TIMESTAMP_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 24 hour)
                    -- OR data_fim IS NULL # data_fim não está confiável, temos status fechados sem esse campo
                    -- )
            WHERE  intersected_areas.row_num = 1
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
        # order by qnt_alagamentos
        """,
        "query_update": """
            SELECT date_trunc(current_datetime("America/Sao_Paulo"), minute) AS last_update
        """,
    }
