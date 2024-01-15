# -*- coding: utf-8 -*-
"""
Flows for setting rain data in Redis.
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio.rain_dashboard.constants import (
    constants as rain_dashboard_constants,
)

# from pipelines.rj_escritorio.rain_dashboard.schedules import every_fifteen_minutes
from pipelines.rj_escritorio.rain_dashboard.tasks import (
    dataframe_to_dict,
    get_data,
    set_redis_key,
)
from pipelines.utils.decorators import Flow

with Flow(
    name=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_NAME.value,
    code_owners=[
        "gabriel",
    ],
    # skip_if_running=True,
) as rj_escritorio_rain_dashboard_flow:
    # Parameters
    query_data = Parameter("query_data")
    query_update = Parameter("query_update")
    mode = Parameter("mode", default="prod")
    redis_data_key = Parameter("redis_data_key", default="data_last_15min_rain")
    redis_update_key = Parameter(
        "redis_update_key", default="data_last_15min_rain_update"
    )
    redis_host = Parameter("redis_host", default="redis.redis.svc.cluster.local")
    redis_port = Parameter("redis_port", default=6379)
    redis_db = Parameter("redis_db", default=1)

    # Tasks
    dataframe = get_data(query=query_data, mode=mode)
    dataframe_update = get_data(query=query_update, mode=mode)
    dictionary = dataframe_to_dict(dataframe=dataframe)
    dictionary_update = dataframe_to_dict(dataframe=dataframe_update)
    set_redis_key(
        key=redis_data_key,
        value=dictionary,
        host=redis_host,
        port=redis_port,
        db=redis_db,
    )
    set_redis_key(
        key=redis_update_key,
        value=dictionary_update,
        host=redis_host,
        port=redis_port,
        db=redis_db,
    )


rj_escritorio_rain_dashboard_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio_rain_dashboard_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
# rj_escritorio_rain_dashboard_flow.schedule = every_fifteen_minutes
