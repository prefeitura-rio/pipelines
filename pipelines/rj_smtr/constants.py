# -*- coding: utf-8 -*-
"""
Constant values for the rj_smtr projects
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the rj_smtr projects
    """

    # DEFAULT TIMEZONE #
    TIMEZONE = "America/Sao_Paulo"

    # WEBHOOK #
    CRITICAL_SECRET_PATH = "critical_webhook"

    # RETRY POLICY #
    MAX_TIMEOUT_SECONDS = 60
    MAX_RETRIES = 3
    RETRY_DELAY = 10

    # GPS STPL #
    GPS_STPL_API_BASE_URL = "http://zn4.m2mcontrol.com.br/api/integracao/veiculos"
    GPS_STPL_API_SECRET_PATH = "stpl_api"

    GPS_STPL_DATASET_ID = "br_rj_riodejaneiro_veiculos"
    GPS_STPL_RAW_DATASET_ID = "br_rj_riodejaneiro_stpl_gps"
    GPS_STPL_RAW_TABLE_ID = "registros"
    GPS_STPL_TREATED_TABLE_ID = "gps_stpl"

    # GPS SPPO #
    GPS_SPPO_API_BASE_URL = (
        "http://ccomobility.com.br/WebServices/Binder/WSConecta/EnvioInformacoesIplan?"
    )
    GPS_SPPO_API_BASE_URL_V2 = (
        "http://ccomobility.com.br/WebServices/Binder/wsconecta/EnvioIplan?"
    )
    GPS_SPPO_API_SECRET_PATH = "sppo_api"
    GPS_SPPO_API_SECRET_PATH_V2 = "sppo_api_v2"

    GPS_SPPO_RAW_DATASET_ID = "br_rj_riodejaneiro_onibus_gps"
    GPS_SPPO_RAW_TABLE_ID = "registros"
    GPS_SPPO_DATASET_ID = "br_rj_riodejaneiro_veiculos"
    GPS_SPPO_TREATED_TABLE_ID = "gps_sppo"
    GPS_SPPO_CAPTURE_DELAY_V1 = 1
    GPS_SPPO_CAPTURE_DELAY_V2 = 60
    GPS_SPPO_RECAPTURE_DELAY_V2 = 6
    GPS_SPPO_MATERIALIZE_DELAY_HOURS = 1

    # REALOCAÇÃO #
    GPS_SPPO_REALOCACAO_RAW_TABLE_ID = "realocacao"
    GPS_SPPO_REALOCACAO_TREATED_TABLE_ID = "realocacao"
    GPS_SPPO_REALOCACAO_SECRET_PATH = "realocacao_api"

    # GPS BRT #
    GPS_BRT_API_SECRET_PATH = "brt_api_v2"
    GPS_BRT_API_URL = "https://zn4.m2mcontrol.com.br/api/integracao/veiculos"
    GPS_BRT_DATASET_ID = "br_rj_riodejaneiro_veiculos"
    GPS_BRT_RAW_DATASET_ID = "br_rj_riodejaneiro_brt_gps"
    GPS_BRT_RAW_TABLE_ID = "registros"
    GPS_BRT_TREATED_TABLE_ID = "gps_brt"
    GPS_BRT_MAPPING_KEYS = {
        "codigo": "id_veiculo",
        "linha": "servico",
        "latitude": "latitude",
        "longitude": "longitude",
        "dataHora": "timestamp_gps",
        "velocidade": "velocidade",
        "sentido": "sentido",
        "trajeto": "vista",
        # "inicio_viagem": "timestamp_inicio_viagem",
    }
    GPS_BRT_MATERIALIZE_DELAY_HOURS = 0

    # SIGMOB (GTFS) #
    SIGMOB_GET_REQUESTS_TIMEOUT = 60
    SIGMOB_PAGES_FOR_CSV_FILE = 10
    TASK_MAX_RETRIES = 3
    TASK_RETRY_DELAY = 10

    SIGMOB_DATASET_ID = "br_rj_riodejaneiro_sigmob"
    SIGMOB_ENDPOINTS = {
        "agency": {
            "url": "http://jeap.rio.rj.gov.br/MOB/get_agency.rule?sys=MOB",
            "key_column": "agency_id",
        },
        "calendar": {
            "url": "http://jeap.rio.rj.gov.br/MOB/get_calendar.rule?sys=MOB",
            "key_column": "service_id",
        },
        "frota_determinada": {
            "url": "http://jeap.rio.rj.gov.br/MOB/get_frota_determinada.rule?sys=MOB",
            "key_column": "route_id",
        },
        "holidays": {
            "url": "http://jeap.rio.rj.gov.br/MOB/get_holiday.rule?sys=MOB",
            "key_column": "Data",
        },
        "linhas": {
            "url": "http://jeap.rio.rj.gov.br/MOB/get_linhas.rule?sys=MOB",
            "key_column": "linha_id",
        },
        "routes": {
            "url": "http://jeap.rio.rj.gov.br/MOB/get_routes.rule?sys=MOB",
            "key_column": "route_id",
        },
        "shapes": {
            "url": "http://jeap.rio.rj.gov.br/MOB/get_shapes.rule?sys=MOB&INDICE=0",
            "key_column": "shape_id",
        },
        "stops": {
            "url": "http://jeap.rio.rj.gov.br/MOB/get_stops.rule?sys=MOB&INDICE=0",
            "key_column": "stop_id",
        },
        "stop_times": {
            "url": "http://jeap.rio.rj.gov.br/MOB/get_stop_times.rule?sys=MOB",
            "key_column": "stop_id",
        },
        "stop_details": {
            "url": "http://jeap.rio.rj.gov.br/MOB/get_stops_details.rule?sys=MOB&INDICE=0",
            "key_column": "stop_id",
        },
        "trips": {
            "url": "http://jeap.rio.rj.gov.br/MOB/get_trips.rule?sys=MOB",
            "key_column": "trip_id",
        },
    }

    # RDO/RHO
    RDO_FTP_ALLOWED_PATHS = ["SPPO", "STPL"]
    RDO_FTPS_SECRET_PATH = "smtr_rdo_ftps"
    RDO_DATASET_ID = "br_rj_riodejaneiro_rdo"
    SPPO_RDO_TABLE_ID = "rdo_registros_sppo"
    SPPO_RHO_TABLE_ID = "rho_registros_sppo"
    STPL_RDO_TABLE_ID = "rdo_registros_stpl"
    STPL_RHO_TABLE_ID = "rho_registros_stpl"
    RDO_MATERIALIZE_START_DATE = "2022-12-07"
    # ROCK IN RIO
    RIR_DATASET_ID = "dashboards"
    RIR_TABLE_ID = "registros_ocr_rir"
    RIR_START_DATE = "2022-08-30 12:00:00"
    RIR_SECRET_PATH = "smtr_rir_ftp"
    RIR_OCR_PRIMARY_COLUMNS = {
        "CodCET": "codigo_cet",
        "Placa": "placa",
        "UF": "uf",
        "LOCAL": "local",
        "datahora": "datahora",
    }
    RIR_OCR_SECONDARY_COLUMNS = {
        "RiR": "flag_rir",
        "Apoio": "flag_apoio",
    }

    # SUBSÍDIO
    SUBSIDIO_SPPO_DATASET_ID = "projeto_subsidio_sppo"
    SUBSIDIO_SPPO_TABLE_ID = "viagem_completa"

    # SUBSÍDIO DASHBOARD
    SUBSIDIO_SPPO_DASHBOARD_DATASET_ID = "dashboard_subsidio_sppo"
    SUBSIDIO_SPPO_DASHBOARD_TABLE_ID = "sumario_servico_dia"

    # BILHETAGEM
    BILHETAGEM_DATASET_ID = "br_rj_riodejaneiro_bilhetagem"

    BILHETAGEM_GENERAL_CAPTURE_PARAMS = {
        "databases": {
            "principal_db": {
                "engine": "mysql",
                "host": "10.5.114.121",
            },
            "tarifa_db": {
                "engine": "postgresql",
                "host": "10.5.113.254",
            },
            "transacao_db": {
                "engine": "postgresql",
                "host": "10.5.115.1",
            },
            "tracking_db": {
                "engine": "postgresql",
                "host": "10.5.15.25",
            },
            "ressarcimento_db": {
                "engine": "postgresql",
                "host": "10.5.15.127",
            },
        },
        "source_type": "db",
    }

    BILHETAGEM_TRANSACAO_CAPTURE_PARAMS = {
        "table_id": "transacao",
        "partition_date_only": False,
        "extract_params": {
            "database": "transacao_db",
            "query": """
                SELECT
                    *
                FROM
                    transacao
                WHERE
                    data_processamento BETWEEN '{start}'
                    AND '{end}'
            """,
        },
        "primary_key": ["id"],
        "interval_minutes": 1,
    }

    BILHETAGEM_TRACKING_CAPTURE_PARAMS = {
        "table_id": "gps_validador",
        "partition_date_only": False,
        "extract_params": {
            "database": "tracking_db",
            "query": """
                SELECT
                    *
                FROM
                    tracking_detalhe
                WHERE
                    id > {last_id} AND id <= {max_id}
            """,
            "page_size": 1000,
            "max_pages": 100,
        },
        "primary_key": ["id"],
        "interval_minutes": 5,
    }

    BILHETAGEM_ORDEM_PAGAMENTO_CAPTURE_PARAMS = [
        {
            "table_id": "ordem_ressarcimento",
            "partition_date_only": True,
            "extract_params": {
                "database": "ressarcimento_db",
                "query": """
                SELECT
                    *
                FROM
                    ordem_ressarcimento
                WHERE
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            },
            "primary_key": ["id"],
            "interval_minutes": 1440,
        },
        {
            "table_id": "ordem_pagamento",
            "partition_date_only": True,
            "extract_params": {
                "database": "ressarcimento_db",
                "query": """
                SELECT
                    *
                FROM
                    ordem_pagamento
                WHERE
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            },
            "primary_key": ["id"],
            "interval_minutes": 1440,
        },
    ]

    BILHETAGEM_SECRET_PATH = "smtr_jae_access_data"

    BILHETAGEM_TRATAMENTO_INTERVAL = 60

    BILHETAGEM_PRIVATE_BUCKET = "rj-smtr-jae-private"

    BILHETAGEM_CAPTURE_PARAMS = [
        {
            "table_id": "linha",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        *
                    FROM
                        LINHA
                    WHERE
                        DT_INCLUSAO BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": ["CD_LINHA"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "grupo",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        *
                    FROM
                        GRUPO
                    WHERE
                        DT_INCLUSAO BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": ["CD_GRUPO"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "grupo_linha",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        *
                    FROM
                        GRUPO_LINHA
                    WHERE
                        DT_INCLUSAO BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": ["CD_GRUPO", "CD_LINHA"],
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "matriz_integracao",
            "partition_date_only": True,
            "extract_params": {
                "database": "tarifa_db",
                "query": """
                    SELECT
                        *
                    FROM
                        matriz_integracao
                    WHERE
                        dt_inclusao BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": [
                "cd_versao_matriz",
                "cd_integracao",
            ],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "operadora_transporte",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        o.*,
                        m.DS_TIPO_MODAL
                    FROM
                        OPERADORA_TRANSPORTE o
                    LEFT JOIN
                        TIPO_MODAL m
                    ON
                        o.CD_TIPO_MODAL = m.CD_TIPO_MODAL
                    WHERE
                        DT_INCLUSAO BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": ["CD_OPERADORA_TRANSPORTE"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "cliente",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        c.*
                    FROM
                        CLIENTE c
                    JOIN
                        OPERADORA_TRANSPORTE o
                    ON
                        c.CD_CLIENTE = o.CD_CLIENTE
                    WHERE
                        DT_CADASTRO BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": ["CD_CLIENTE"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
            "save_bucket_name": BILHETAGEM_PRIVATE_BUCKET,
            "pre_treatment_reader_args": {"dtype": {"NR_DOCUMENTO": "object"}},
        },
        {
            "table_id": "consorcio",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        *
                    FROM
                        CONSORCIO
                    WHERE
                        DT_INCLUSAO BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": ["CD_CONSORCIO"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "linha_consorcio",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        *
                    FROM
                        LINHA_CONSORCIO
                    WHERE
                        DT_INCLUSAO BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": ["CD_CONSORCIO", "CD_LINHA"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "conta_bancaria",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        c.*,
                        b.NM_BANCO
                    FROM
                        CONTA_BANCARIA c
                    JOIN
                        BANCO b
                    ON
                        b.NR_BANCO = c.NR_BANCO
                    JOIN
                        OPERADORA_TRANSPORTE o
                    ON
                        o.CD_CLIENTE = c.CD_CLIENTE
                """,
            },
            "primary_key": ["CD_CLIENTE"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
            "save_bucket_name": BILHETAGEM_PRIVATE_BUCKET,
        },
        {
            "table_id": "contato_pessoa_juridica",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        *
                    FROM
                        CONTATO_PESSOA_JURIDICA
                    WHERE
                        DT_INCLUSAO BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": [
                "NR_SEQ_CONTATO",
                "CD_CLIENTE",
            ],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
            "save_bucket_name": BILHETAGEM_PRIVATE_BUCKET,
        },
    ]

    BILHETAGEM_MATERIALIZACAO_TRANSACAO_PARAMS = {
        "dataset_id": BILHETAGEM_DATASET_ID,
        "table_id": BILHETAGEM_TRANSACAO_CAPTURE_PARAMS["table_id"],
        "upstream": True,
        "dbt_vars": {
            "date_range": {
                "table_run_datetime_column_name": "datetime_transacao",
                "delay_hours": 1,
            },
            "version": {},
        },
    }

    BILHETAGEM_MATERIALIZACAO_ORDEM_PAGAMENTO_PARAMS = {
        "dataset_id": BILHETAGEM_DATASET_ID,
        "table_id": "ordem_pagamento",
        "upstream": True,
        "exclude": f"+{BILHETAGEM_MATERIALIZACAO_TRANSACAO_PARAMS['table_id']}",
        "dbt_vars": {
            "date_range": {
                "table_run_datetime_column_name": "data_ordem",
                "delay_hours": 0,
            },
            "version": {},
        },
    }

    BILHETAGEM_GENERAL_CAPTURE_DEFAULT_PARAMS = {
        "dataset_id": BILHETAGEM_DATASET_ID,
        "secret_path": BILHETAGEM_SECRET_PATH,
        "source_type": BILHETAGEM_GENERAL_CAPTURE_PARAMS["source_type"],
    }

    # GTFS
    GTFS_DATASET_ID = "br_rj_riodejaneiro_gtfs"

    GTFS_GENERAL_CAPTURE_PARAMS = {
        "partition_date_only": True,
        "source_type": "gcs",
        "dataset_id": "br_rj_riodejaneiro_gtfs",
        "extract_params": {"filename": "gtfs"},
        "partition_date_name": "data_versao",
    }

    GTFS_TABLE_CAPTURE_PARAMS = [
        {
            "table_id": "shapes",
            "primary_key": ["shape_id", "shape_pt_sequence"],
        },
        {
            "table_id": "agency",
            "primary_key": ["agency_id"],
        },
        {
            "table_id": "calendar_dates",
            "primary_key": ["service_id", "date"],
        },
        {
            "table_id": "calendar",
            "primary_key": ["service_id"],
        },
        {
            "table_id": "feed_info",
            "primary_key": ["feed_publisher_name"],
        },
        {
            "table_id": "frequencies",
            "primary_key": ["trip_id", "start_time"],
        },
        {
            "table_id": "routes",
            "primary_key": ["route_id"],
        },
        {
            "table_id": "stops",
            "primary_key": ["stop_id"],
        },
        {
            "table_id": "trips",
            "primary_key": ["trip_id"],
        },
        {
            "table_id": "fare_attributes",
            "primary_key": ["fare_id"],
        },
        {
            "table_id": "fare_rules",
            "primary_key": [],
        },
        {
            "table_id": "ordem_servico",
            "primary_key": ["servico"],
            "extract_params": {"filename": "ordem_servico"},
        },
        {
            "table_id": "stop_times",
            "primary_key": ["trip_id", "stop_sequence"],
        },
    ]

    GTFS_MATERIALIZACAO_PARAMS = {
        "dataset_id": GTFS_DATASET_ID,
        "dbt_vars": {
            "data_versao_gtfs": "",
            "version": {},
        },
    }

    # STU

    STU_DATASET_ID = "br_rj_riodejaneiro_stu"

    STU_BUCKET_NAME = "rj-smtr-stu-private"

    STU_MODE_MAPPING = {
        "1": "Táxi",
        "2": "Ônibus",
        "3": "Escolar",
        "4": "Complementar (cabritinho)",
        "6": "Fretamento",
        "7": "TEC",
        "8": "Van",
    }

    STU_TYPE_MAPPING = [
        "Autônomo",
        "Empresa",
        "Cooperativa",
        "Instituicao de Ensino",
        "Associações",
        "Autônomo Provisório",
        "Contrato Público",
        "Prestadora de Serviços",
    ]

    STU_GENERAL_CAPTURE_PARAMS = {
        "partition_date_only": True,
        "source_type": "gcs",
        "dataset_id": STU_DATASET_ID,
        "save_bucket_name": STU_BUCKET_NAME,
    }

    STU_TABLE_CAPTURE_PARAMS = [
        {
            "table_id": "operadora_empresa",
            "primary_key": ["Perm_Autor"],
            "pre_treatment_reader_args": {"dtype": "object"},
        },
        {
            "table_id": "operadora_pessoa_fisica",
            "primary_key": ["Perm_Autor"],
            "pre_treatment_reader_args": {"dtype": "object"},
        },
    ]

    # SUBSÍDIO RECURSOS VIAGENS INDIVIDUAIS

    SUBSIDIO_SPPO_RECURSOS_DATASET_ID = "br_rj_riodejaneiro_recursos"
    SUBSIDIO_SPPO_RECURSO_API_BASE_URL = "https://api.movidesk.com/public/v1/tickets"
    SUBSIDIO_SPPO_RECURSO_API_SECRET_PATH = "sppo_subsidio_recursos_api"
    SUBSIDIO_SPPO_RECURSO_SERVICE = (
        "serviceFirstLevel eq 'Viagem Individual - Recurso Viagens Subsídio'"
    )
    SUBSIDIO_SPPO_RECURSO_CAPTURE_PARAMS = {
        "partition_date_only": True,
        "table_id": "recursos_sppo_viagens_individuais",
        "dataset_id": SUBSIDIO_SPPO_RECURSOS_DATASET_ID,
        "extract_params": {
            "token": "",
            "$select": "id,protocol,createdDate,lastUpdate",
            "$filter": "{service} and (lastUpdate ge {start} and lastUpdate lt {end} \
or createdDate ge {start} and createdDate lt {end})",
            "$expand": "customFieldValues,customFieldValues($expand=items)",
            "$orderby": "createdDate asc",
        },
        "interval_minutes": 1440,
        "source_type": "movidesk",
        "primary_key": ["protocol"],
    }

    SUBSIDIO_SPPO_RECURSOS_MATERIALIZACAO_PARAMS = {
        "dataset_id": SUBSIDIO_SPPO_RECURSOS_DATASET_ID,
        "table_id": SUBSIDIO_SPPO_RECURSO_CAPTURE_PARAMS["table_id"],
        "upstream": True,
        "dbt_vars": {
            "date_range": {
                "table_run_datetime_column_name": "datetime_recurso",
                "delay_hours": 0,
            },
            "version": {},
        },
    }

    DIRETORIO_MATERIALIZACAO_PARAMS = {
        "dataset_id": "cadastro",
        "upstream": True,
    }

    DIRETORIO_MATERIALIZACAO_TABLE_PARAMS = [
        {"table_id": "diretorio_consorcios"},
        {"table_id": "operadoras_contatos"},
    ]
