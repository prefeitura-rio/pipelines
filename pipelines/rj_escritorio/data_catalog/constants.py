# -*- coding: utf-8 -*-
from enum import Enum


class constants(Enum):
    """
    Constant values for the data catalog flow
    """

    ARCGIS_CREDENTIALS_SECRET_PATH = "arcgis_credentials"
    DONT_PUBLISH = ["datario.dados_mestres.bairro", "datario.dados_mestres.logradouro"]
    GCS_BUCKET_NAME = "datario-public"
    DESCRIPTION_HTML_TEMPLATE_PATH = (
        "datario-public/templates/datario_description.html.jinja"
    )
