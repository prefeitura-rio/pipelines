# -*- coding: utf-8 -*-
"""
Constant values for br_rj_riodejaneiro_brt_prediction
"""

from enum import Enum
import os


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_rj_riodejaneiro_brt_prediction project
    """

    TIMEZONE = "America/Sao_Paulo"

    CURRENT_DIR = os.path.dirname(__file__)

    BRT_PREDICTOR_BUCKET = "gs://rj-smtr-dev/brt-predictor"

    SMTR_BUCKET = "rj-smtr-dev"

    TRIP_ID_INDICE_SENTIDO = 11

    NEAR_STOP_THRESHOLD = 400

    ON_STOP_THRESHOLD = 100

    MAXIMUM_SECONDS_BETWEEN_STOPS = 60 * 90
