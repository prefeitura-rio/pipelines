# -*- coding: utf-8 -*-
"""
Constant values for the rj_smtr.operacao flows
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the rj_smtr.operacao flows
    """

    DATASET_ID = "operacao"

    # VEÍCULOS LICENCIADOS
    SPPO_INFRACAO_TABLE_ID = "sppo_infracao"
    SPPO_INFRACAO_MAPPING_KEYS = {
        "permissao": "permissao",
        "modal": "modo",
        "placa": "placa",
        "cm": "id_auto_infracao",
        "data_infracao": "data_infracao",
        "valor": "valor",
        "cod_infracao": "id_infracao",
        "des_infracao": "infracao",
        "status": "status",
        "data_pagamento": "data_pagamento",
    }
