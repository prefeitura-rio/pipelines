# -*- coding: utf-8 -*-
# pylint: disable=c0103
"""
Constant values for the satelite tables
"""

from enum import Enum


class constants(Enum):
    """
    Constant values for the satelite project
    """

    DATASET_ID = "clima_satelite"

    VARIAVEL_RR = "RRQPEF"
    TABLE_ID_RR = "taxa_precipitacao_goes_16_temp"
    VARIAVEL_TPW = "TPWF"
    TABLE_ID_TPW = "quantidade_agua_precipitavel_goes_16"
    VARIAVEL_cmip = "CMIPF"
    BAND_CMIP_13 = "13"
    TABLE_ID_cmip = "infravermelho_longo_banda_13_goes_16_temp"
    # https://developers.google.com/earth-engine/datasets/catalog/NOAA_GOES_16_MCMIPM#bands
    VARIAVEL_mcmip = "MCMIPF"
    TABLE_ID_mcmip = "imagem_nuvens_umidade_goes_16_temp"
