# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for utils.
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the dump vitai flows
    """

    VAULT_PATH = "prontuario_vitacare"
    DATASET_ID = "brutos_prontuario_vitacare"
    BASE_URL = {
        "10" : "http://consolidado-ap10.pepvitacare.com:8088",
        "21" : "http://consolidado-ap21.pepvitacare.com:8088",
        "22" : "http://consolidado-ap22.pepvitacare.com:8088",
        "31" : "http://consolidado-ap31.pepvitacare.com:8089",
        "32" : "http://consolidado-ap32.pepvitacare.com:8088",
        "33" : "http://consolidado-ap33.pepvitacare.com:8089",
        "40" : "http://consolidado-ap40.pepvitacare.com:8089",
        "51" : "http://consolidado-ap51.pepvitacare.com:8089",
        "52" : "http://consolidado-ap52.pepvitacare.com:8088",
        "53" : "http://consolidado-ap53.pepvitacare.com:8090",
    }
    ENDPOINT = {
        "posicao" : "/reports/pharmacy/stocks",
        "movimento" : "/reports/pharmacy/movements"
    }
