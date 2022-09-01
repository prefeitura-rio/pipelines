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

    GPS_STPL_DATASET_ID = "br_rj_riodejaneiro_stpl_gps"
    GPS_STPL_RAW_TABLE_ID = "registros"
    # GPS_STPL_TREATED_TABLE_ID = ...

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
    # GPS BRT #
    GPS_BRT_API_BASE_URL = (
        "http://citgisbrj.tacom.srv.br:9977/gtfs-realtime-exporter/findAll/json"
    )
    # GPS_BRT_API_SECRET_PATH = "sppo_api"

    GPS_BRT_DATASET_ID = "br_rj_riodejaneiro_veiculos"
    GPS_BRT_RAW_DATASET_ID = "br_rj_riodejaneiro_brt_gps"
    GPS_BRT_RAW_TABLE_ID = "registros"
    GPS_BRT_TREATED_TABLE_ID = "gps_brt"

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
