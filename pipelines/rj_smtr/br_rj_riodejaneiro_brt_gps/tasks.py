# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_brt_gps
"""

from datetime import timedelta
import traceback
import pandas as pd
from prefect import task

# EMD Imports #

from pipelines.utils.utils import get_vault_secret, log

# SMTR Imports #

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.utils import log_critical, map_dict_keys

# Tasks #


@task
def create_api_url_brt_gps(secret_path: str = constants.GPS_BRT_SECRET_PATH.value):
    """Create the url to request data from

    Args:
        secret_path (str, optional): secret path to the url.

    Returns:
        _str: url to request
    """
    url = get_vault_secret(secret_path=secret_path)["data"]["url"]
    log(f"Request data from {url}")
    return url


@task
def pre_treatment_br_rj_riodejaneiro_brt_gps(status: dict, timestamp):
    """Basic data treatment for brt gps data. Converts unix time to datetime,
    and apply filtering to stale data that may populate the API response.

    Args:
        status_dict (dict): dict containing the status of the request made to the
        API. Must contain keys: data, timestamp and error

    Returns:
        df: pandas.core.DataFrame containing the treated data.
    """

    # Check previous error
    if status["error"] is not None:
        return {"data": pd.DataFrame(), "error": status["error"]}

    error = None
    data = status["data"]
    timezone = constants.TIMEZONE.value
    log(
        f"""
    Received inputs:
    - timestamp:\n{timestamp}
    - data:\n{data[:10]}"""
    )
    # Create dataframe sctructure
    key_column = "id_veiculo"
    columns = [key_column, "timestamp_gps", "timestamp_captura", "content"]
    df = pd.DataFrame(columns=columns)  # pylint: disable=c0103

    # Populate dataframe

    # map_dict_keys change data keys to match project data structure
    map_keys = {
        "vei_nro_gestor": "id_veiculo",
        "linha": "servico",
        "latitude": "latitude",
        "longitude": "longitude",
        "comunicacao": "timestamp_gps",
        "velocidade": "velocidade",
        "nomeItinerario": "sentido",
    }
    df["content"] = [map_dict_keys(piece, map_keys) for piece in data]
    df[key_column] = [piece[key_column] for piece in data]
    df["timestamp_gps"] = [piece["timestamp_gps"] for piece in data]
    df["timestamp_captura"] = timestamp
    log(f"timestamp captura is:\n{df['timestamp_captura']}")
    # Remove timezone and force it to be config timezone
    log(f"Before converting, timestamp_gps is: \n{df['timestamp_gps']}")

    df["timestamp_gps"] = (
        pd.to_datetime(df["timestamp_gps"], unit="s")
        .dt.tz_localize("UTC")
        .dt.tz_convert(timezone)
    )
    log(f"After converting the timezone, timestamp_gps is: \n{df['timestamp_gps']}")
    # log(f"df is {df}")

    # Filter data for 0 <= time diff <= 1min
    try:
        log(f"Shape antes da filtragem: {df.shape}")

        mask = (df["timestamp_captura"] - df["timestamp_gps"]) <= timedelta(minutes=1)
        df = df[mask]  # pylint: disable=C0103
        log(f"Shape após a filtragem: {df.shape}")
        if df.shape[0] == 0:
            error = ValueError("After filtering, the dataframe is empty!")
            log_critical(f"@here\nFailed to filter BRT data: \n{error}")
    except Exception:  # pylint: disable = W0703
        err = traceback.format_exc()
        log_critical(f"@here\nFailed to filter BRT data: \n{err}")

    return {"data": df, "error": error}
