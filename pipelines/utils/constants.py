# -*- coding: utf-8 -*-
"""
Constants for utils.
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants for utils.
    """

    FLOW_BACKFILL_NAME = "EMD: backfill - Faz o backfill de flows genéricos"
    FLOW_EXECUTE_DBT_MODEL_NAME = "EMD: template - Executa DBT model"
    FLOW_DUMP_DB_NAME = "EMD: template - Ingerir tabela de banco SQL"
    FLOW_DUMP_DATARIO_NAME = "EMD: template - Ingerir tabela do data.rio"
    FLOW_DUMP_EARTH_ENGINE_ASSET_NAME = "EMD: template - Criar asset no Earth Engine"
    FLOW_DUMP_TO_GCS_NAME = "EMD: template - Ingerir tabela zipada para GCS"
    FLOW_DUMP_URL_NAME = "EMD: template - Ingerir tabela de URL"
    FLOW_PREDICT_NAME = "EMD: template - Faz predição com modelo do MLflow"
    FLOW_SEND_WHATSAPP_MESSAGE_NAME = "EMD: template - Enviar mensagem via Whatsapp Bot"
