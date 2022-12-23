# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_brt_prediction
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.rj_smtr.br_rj_riodejaneiro_brt_prediction.constants import (
    constants as local_constants,
)
import pendulum
from datetime import timedelta, date

from pipelines.rj_smtr.br_rj_riodejaneiro_brt_prediction.tasks import (
    preprocess_brt_stops,
    preprocess_frequencies_info,
    preprocess_shapes_stops,
    recover_historical_brt_registers,
    extract_historical_brt_intervals,
    create_median_model,
)

# from pipelines.rj_smtr.br_rj_riodejaneiro_brt_prediction.schedules import every_two_weeks
from pipelines.utils.decorators import Flow

with Flow(
    "SMTR - Gerar Predicao do BRT", code_owners=["ronald", "vinicius", "joao_pedro"]
) as generate_brt_prediction:

    today = pendulum.now(local_constants.TIMEZONE.value).date()

    # TODO: PRECISA SER A PARTIR DE HOJE MAS O BIGQUERY TA SEM DADO
    last_date = date(2022, 9, 1)
    # last_date = today - timedelta(days=1)
    first_date = last_date - timedelta(days=30 * 2)

    preprocess_brt_stops()
    preprocess_frequencies_info()
    preprocess_shapes_stops()
    recover_historical_brt_registers(first_date, last_date)
    extract_historical_brt_intervals()
    create_median_model()

generate_brt_prediction.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
generate_brt_prediction.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow.schedule = every_two_weeks
