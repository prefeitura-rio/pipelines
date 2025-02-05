# -*- coding: utf-8 -*-
"""
Constant values for rj_smtr controle_financeiro
"""

from enum import Enum
from pipelines.rj_smtr.constants import constants as smtr_constants


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for rj_smtr controle_financeiro
    """

    SHEETS_BASE_URL = "https://docs.google.com/spreadsheets/d/\
1QVfa9b8jzpQr3gac0FIlozmTaVeArtJROA343A2lMVM/export?format=csv&gid="

    SHEETS_CAPTURE_DEFAULT_PARAMS = {
        "dataset_id": smtr_constants.CONTROLE_FINANCEIRO_DATASET_ID.value,
        "source_type": "api-csv",
        "partition_date_only": True,
    }

    SHEETS_CB_CAPTURE_PARAMS = {
        "extract_params": {
            "sheet_id": "454453523",
            "base_url": SHEETS_BASE_URL,
        },
        "table_id": "cb",
    }
    SHEETS_CETT_CAPTURE_PARAMS = {
        "extract_params": {
            "sheet_id": "0",
            "base_url": SHEETS_BASE_URL,
        },
        "table_id": "cett",
    }

    ARQUIVO_RETORNO_TABLE_ID = "arquivo_retorno"

    CCT_API_SECRET_PATH = "cct_api"
    CCT_API_BASE_URL = "https://api.cct.mobilidade.rio/api/v1"
