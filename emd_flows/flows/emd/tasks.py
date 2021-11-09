"""
Tasks for all flows
"""
from datetime import datetime
from os import getenv
from random import choice

import pandas as pd
from prefect import task
import requests

API_URL = "http://webapibrt.rio.rj.gov.br/api/v1/brt"
API2_URL = "http://ccomobility.com.br/WebServices/Binder/WSConecta/EnvioInformacoesIplan?guidIdentificacao=994142d7-2223-4044-931e-be3421ea02ff"
API_TIMEOUT = 60
DISCORD_HOOK = getenv("DISCORD_HOOK")


@task
def get_random_api() -> str:
    """
    Get a random API
    """
    return choice([API_URL, API2_URL])


@task
def fetch_from_api(api_url: str) -> list:
    """
    Fetch data from API
    """
    try:
        response = requests.get(api_url, timeout=API_TIMEOUT)
        response.raise_for_status()
        return response.json()["veiculos"]
    except Exception:
        return []


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
            },{wf_name},timed out or something else"""
        }
    else:
        payload = {
            "content": f"""{
                timestamp.strftime('%Y-%m-%d %H:%M:%S')
            },{wf_name},{dataframe.shape[0]} rows"""
        }
    requests.post(DISCORD_HOOK, data=payload)
