# -*- coding: utf-8 -*-
"""
Constant values for br_rj_riodejaneiro_onibus_gps_zirix
"""

from enum import Enum
from pipelines.rj_smtr.constants import constants as smtr_constants


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for br_rj_riodejaneiro_onibus_gps_zirix
    """

    VIAGEM_CAPTURE_PARAMETERS = {
        "dataset_id": smtr_constants.VIAGEM_ZIRIX_RAW_DATASET_ID.value,
        "table_id": "viagem_informada",
        "partition_date_only": False,
        "extract_params": {"delay_minutes": 5},
        "primary_key": ["id_viagem"],
        "interval_minutes": 10,
        "source_type": "api-json",
    }
