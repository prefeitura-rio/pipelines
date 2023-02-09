# -*- coding: utf-8 -*-
"""
Flows for setting rain data in Redis.
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio.rain_dashboard.schedules import every_fifteen_minutes
from pipelines.rj_escritorio.rain_dashboard.tasks import (
    dataframe_to_dict,
    get_data,
    set_redis_key,
)
from pipelines.utils.decorators import Flow

with Flow(
    "EMD: Atualizar dados de chuva na api.dados.rio",
    code_owners=[
        "gabriel",
    ],
    skip_if_running=True,
) as rj_escritorio_rain_dashboard_flow:
    # Parameters
    query = Parameter("query")
    mode = Parameter("mode", default="prod")
    redis_data_key = Parameter("redis_data_key", default="data_last_15min_rain")
    redis_host = Parameter("redis_host", default="redis.redis.svc.cluster.local")
    redis_port = Parameter("redis_port", default=6379)
    redis_db = Parameter("redis_db", default=1)

    # Tasks
    dataframe = get_data(query=query, mode=mode)
    dictionary = dataframe_to_dict(dataframe=dataframe)
    set_redis_key(
        key=redis_data_key,
        value=dictionary,
        host=redis_host,
        port=redis_port,
        db=redis_db,
    )


rj_escritorio_rain_dashboard_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio_rain_dashboard_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
rj_escritorio_rain_dashboard_flow.schedule = every_fifteen_minutes
