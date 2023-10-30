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

    VAULT_PATH = "estoque_vitai"
    VAULT_KEY = "token"
    DATASET_ID = "brutos_prontuario_vitacare"
    TABLE_POSICAO_ID = "estoque_posicao"
    TABLE_MOVIMENTOS_ID = "estoque_movimento"
    ENDPOINT_BASE_URL = [
        "http://consolidado-ap10.pepvitacare.com:8088",
        "http://consolidado-ap21.pepvitacare.com:8088",
        "http://consolidado-ap22.pepvitacare.com:8088",
        "http://consolidado-ap31.pepvitacare.com:8089",
        "http://consolidado-ap32.pepvitacare.com:8088",
        "http://consolidado-ap33.pepvitacare.com:8089",
        "http://consolidado-ap40.pepvitacare.com:8089",
        "http://consolidado-ap51.pepvitacare.com:8089",
        "http://consolidado-ap52.pepvitacare.com:8088",
        "http://consolidado-ap53.pepvitacare.com:8090"]
    ENDPOINT_POSICAO = "/reports/pharmacy/stocks"
    ENDPOINT_MOVIMENTOS = "/reports/pharmacy/movements"