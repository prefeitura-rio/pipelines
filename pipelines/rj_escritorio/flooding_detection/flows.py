# -*- coding: utf-8 -*-
"""
Flow definition for generating a data catalog from BigQuery.
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

from pipelines.constants import constants
from pipelines.rj_escritorio.flooding_detection.schedules import (
    update_flooding_data_schedule,
)
from pipelines.rj_escritorio.flooding_detection.tasks import (
    get_last_update,
    get_openai_api_key,
    get_prediction,
    get_raining_hexagons,
    get_snapshot,
    pick_cameras,
    update_flooding_api_data,
)
from pipelines.utils.decorators import Flow

with Flow(
    name="EMD: flooding_detection - Atualizar detecção de alagamento (IA) na API",
    code_owners=[
        "gabriel",
        "diego",
    ],
) as rj_escritorio__flooding_detection__flow:
    # Parameters
    openai_flooding_detection_prompt = Parameter(
        "openai_flooding_detection_prompt", required=True
    )
    rain_api_data_url = Parameter(
        "rain_api_url",
        default="https://api.dados.rio/v2/clima_pluviometro/precipitacao_15min/",
    )
    rain_api_update_url = Parameter(
        "rain_api_update_url",
        default="https://api.dados.rio/v2/clima_pluviometro/ultima_atualizacao_precipitacao_15min/",
    )
    redis_key_predictions_buffer = Parameter(
        "redis_key_predictions_buffer", default="flooding_detection_predictions_buffer"
    )
    redis_key_flooding_detection_data = Parameter(
        "redis_key_flooding_detection_data", default="flooding_detection_data"
    )
    redis_key_flooding_detection_last_update = Parameter(
        "redis_key_flooding_detection_last_update",
        default="flooding_detection_last_update",
    )

    # Flow
    hexagons = get_raining_hexagons(rain_api_data_url=rain_api_data_url)
    last_update = get_last_update(rain_api_update_url=rain_api_update_url)
    cameras = pick_cameras(hexagons=hexagons, last_update=last_update)
    openai_api_key = get_openai_api_key()
    images = get_snapshot.map(
        camera=cameras,
    )
    predictions = get_prediction.map(
        image=images,
        flooding_prompt=unmapped(openai_flooding_detection_prompt),
        openai_api_key=unmapped(openai_api_key),
        predictions_buffer_key=unmapped(redis_key_predictions_buffer),
    )
    update_flooding_api_data(
        predictions=predictions,
        cameras=cameras,
        images=images,
        data_key=redis_key_flooding_detection_data,
        last_update_key=redis_key_flooding_detection_last_update,
    )


rj_escritorio__flooding_detection__flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio__flooding_detection__flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_AGENT_LABEL.value],
)
rj_escritorio__flooding_detection__flow.schedule = update_flooding_data_schedule