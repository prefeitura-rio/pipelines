# -*- coding: utf-8 -*-
"""
Schedules for the data catalog pipeline.
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants

update_flooding_data_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=3),
            start_date=datetime(2023, 1, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
            labels=[
                constants.RJ_ESCRITORIO_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "cameras_geodf_url": "https://prefeitura-rio.github.io/storage/cameras_geo_min_bolsao_sample.csv",  # noqa
                "mocked_cameras_number": 0,
                "openai_api_key_secret_path": "openai-api-key-flooding-detection",
                "openai_api_max_tokens": 300,
                "openai_api_model": "gpt-4-vision-preview",
                "openai_api_url": "https://api.openai.com/v1/chat/completions",
                "openai_flooding_detection_prompt": """You are an expert flooding detector. You are
                given a image. You must detect if there is flooding in the image. The output MUST
                be a JSON object with a boolean value for the key "flooding_detected". If you don't
                know what to anwser, you can set the key "flooding_detect" as false. Example:
                {
                    "flooding_detected": true
                }
                """,
                "rain_api_update_url": "https://api.dados.rio/v2/clima_pluviometro/ultima_atualizacao_precipitacao_15min/",  # noqa
                "rain_api_url": "https://api.dados.rio/v2/clima_pluviometro/precipitacao_15min/",
                "redis_key_flooding_detection_data": "flooding_detection_data",
                "redis_key_flooding_detection_last_update": "flooding_detection_last_update",
                "redis_key_predictions_buffer": "flooding_detection_predictions_buffer",
            },
        ),
    ]
)
