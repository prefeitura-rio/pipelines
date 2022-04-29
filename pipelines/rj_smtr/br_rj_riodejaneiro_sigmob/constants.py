# -*- coding: utf-8 -*-
"""
Constant values for the rj_smtr projects
"""


###############################################################################
#
# Esse é um arquivo onde podem ser declaratas constantes que serão usadas
# pelo projeto br_rj_riodejaneiro_sigmob.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar constantes, basta fazer conforme o exemplo abaixo:
#
# ```
# class constants(Enum):
#     """
#     Constant values for the br_rj_riodejaneiro_sigmob project
#     """
#     FOO = "bar"
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.rj_smtr.br_rj_riodejaneiro_sigmob.constants import constants
# print(constants.FOO.value)
# ```
#
###############################################################################

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_rj_riodejaneiro_sigmob project
    """

    SIGMOB_GET_REQUESTS_TIMEOUT = 60
    SIGMOB_PAGES_FOR_CSV_FILE = 10
    TIMEZONE = "America/Sao_Paulo"
    ENDPOINTS = {
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
        # "shapes": {
        #     "url": "http://jeap.rio.rj.gov.br/MOB/get_shapes.rule?sys=MOB&INDICE=0",
        #     "key_column": "shape_id",
        # },
        # "stops": {
        #     "url": "http://jeap.rio.rj.gov.br/MOB/get_stops.rule?sys=MOB&INDICE=0",
        #     "key_column": "stop_id",
        # },
        "stop_times": {
            "url": "http://jeap.rio.rj.gov.br/MOB/get_stop_times.rule?sys=MOB",
            "key_column": "stop_id",
        },
        # "stop_details": {
        #     "url": "http://jeap.rio.rj.gov.br/MOB/get_stops_details.rule?sys=MOB&INDICE=0",
        #     "key_column": "stop_id",
        # },
        "trips": {
            "url": "http://jeap.rio.rj.gov.br/MOB/get_trips.rule?sys=MOB",
            "key_column": "trip_id",
        },
    }
