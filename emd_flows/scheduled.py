"""
Testing scheduled workflows
"""
from datetime import datetime
from os import getenv

import pandas as pd
from prefect import Flow, task
from prefect.run_configs import KubernetesRun
from prefect.storage import Module
import requests

from emd_flows.constants import constants

__all__ = [
    "flow_n1",
    "flow_n2",
    "flow_n3",
    "flow_n4",
    "flow_n5",
]

API_URL = "http://webapibrt.rio.rj.gov.br/api/v1/brt"
API_TIMEOUT = 60
DISCORD_HOOK = getenv("DISCORD_HOOK")


@task
def fetch_from_api() -> list:
    """
    Fetch data from API
    """
    try:
        response = requests.get(API_URL, timeout=API_TIMEOUT)
        response.raise_for_status()
        return response.json()["veiculos"]
    except Exception as e:  # pylint: disable=c0103
        raise e


@task
def csv_to_dataframe(data_list: list) -> pd.DataFrame:
    """
    Convert CSV to DataFrame
    """
    return pd.DataFrame(data_list)


@task
def preproc(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess dataframe
    """
    return dataframe.dropna()


@task
def log_to_discord(dataframe: pd.DataFrame, timestamp: datetime, wf_name: str) -> None:
    """
    Log dataframe to Discord
    """
    if dataframe is None:
        payload = {
            "content": f"""{
                timestamp.strftime('%Y-%m-%d %H:%M:%S')
            } - {wf_name} - timed out or something else"""
        }
    else:
        payload = {
            "content": f"""{
                timestamp.strftime('%Y-%m-%d %H:%M:%S')
            } - {wf_name} - {dataframe.shape[0]} rows"""
        }
    requests.post(DISCORD_HOOK, data=payload)


with Flow("wfn1") as flow_n1:
    ts = datetime.now()
    txt = fetch_from_api()
    df = csv_to_dataframe(txt)
    df = preproc(dataframe=df)
    log_to_discord(dataframe=df, timestamp=ts, wf_name="wf1")

flow_n1.storage = Module("emd_flows.scheduled")

flow_n1.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)


with Flow("wfn2") as flow_n2:
    ts = datetime.now()
    txt = fetch_from_api()
    df = csv_to_dataframe(txt)
    df = preproc(dataframe=df)
    log_to_discord(dataframe=df, timestamp=ts, wf_name="wf2")

flow_n2.storage = Module("emd_flows.scheduled")

flow_n2.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)

with Flow("wfn3") as flow_n3:
    ts = datetime.now()
    txt = fetch_from_api()
    df = csv_to_dataframe(txt)
    df = preproc(dataframe=df)
    log_to_discord(dataframe=df, timestamp=ts, wf_name="wf3")

flow_n3.storage = Module("emd_flows.scheduled")

flow_n3.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)

with Flow("wfn4") as flow_n4:
    ts = datetime.now()
    txt = fetch_from_api()
    df = csv_to_dataframe(txt)
    df = preproc(dataframe=df)
    log_to_discord(dataframe=df, timestamp=ts, wf_name="wf4")

flow_n4.storage = Module("emd_flows.scheduled")

flow_n4.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)

with Flow("wfn5") as flow_n5:
    ts = datetime.now()
    data = fetch_from_api()
    df = csv_to_dataframe(data)
    df = preproc(dataframe=df)
    log_to_discord(dataframe=df, timestamp=ts, wf_name="wf5")

flow_n5.storage = Module("emd_flows.scheduled")

flow_n5.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
