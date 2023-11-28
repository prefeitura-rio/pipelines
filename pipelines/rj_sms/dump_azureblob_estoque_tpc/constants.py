# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa: E501
"""
Constants for TPC.
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the dump vitai flows
    """

    VAULT_PATH = "estoque_tpc"
    VAULT_KEY = "token"
    DATASET_ID = "brutos_estoque_central_tpc"
    CONTAINER_NAME = "datalaketpc"
    BLOB_PATH = {
        "posicao": "gold/logistico/cliente=prefeitura_rio/planta=sms_rio/estoque_local/estoque_local.csv",
        "pedidos": "gold/logistico/cliente=prefeitura_rio/planta=sms_rio/pedidos_depositante/pedidos_depositante.csv",
        "recebimento": "gold/logistico/cliente=prefeitura_rio/planta=sms_rio/recebimento_documental/recebimento_documental.csv",
    }
