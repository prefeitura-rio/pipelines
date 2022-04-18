"""
Testing scheduled workflows
"""

from datetime import datetime

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import Module

from emd_flows.constants import constants
from emd_flows.flows.emd.schedules import (
    minute_schedule,
    five_minute_schedule,
    fifteen_minute_schedule,
)
from emd_flows.flows.emd.tasks import (
    get_random_api,
    fetch_from_api,
    csv_to_dataframe,
    preproc,
    log_to_discord,
)


with Flow("flow_0") as flow_0:
    ts = datetime.now()
    api_url = get_random_api()
    txt = fetch_from_api(api_url)
    df = csv_to_dataframe(txt)
    df = preproc(dataframe=df)
    log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_0")

flow_0.storage = Module("emd_flows.flows")
flow_0.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
flow_0.schedule = fifteen_minute_schedule
