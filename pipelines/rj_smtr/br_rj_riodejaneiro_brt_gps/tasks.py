# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_brt_gps
"""

from datetime import timedelta
import traceback
import pandas as pd
import pendulum
from prefect import task

# EMD Imports #

from pipelines.utils.utils import get_vault_secret, log

# SMTR Imports #

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.utils import log_critical, map_dict_keys, safe_cast

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
def pre_treatment_br_rj_riodejaneiro_brt_gps(status_dict: dict, timestamp):
    """Basic data treatment for brt gps data. Converts unix time to datetime,
    and apply filtering to stale data that may populate the API response.

    Args:
        status_dict (dict): dict containing the status of the request made to the
        API. Must contain keys: data, timestamp and error

    Returns:
        df: pandas.core.DataFrame containing the treated data.
    """

    # Check previous error
    if status_dict["error"] is not None:
        return {"df": pd.DataFrame(), "error": status_dict["error"]}

    error = None
    data = status_dict["data"]
    timezone = constants.TIMEZONE.value
    log(f"Data received to treat:\n{data[:25]}")
    # Create dataframe sctructure
    key_column = "id_veiculo"
    columns = [key_column, "timestamp_gps", "timestamp_captura", "content"]
    df = pd.DataFrame(data, columns=columns)  # pylint: disable=c0103

    # Populate dataframe
    df["timestamp_captura"] = pd.to_datetime(timestamp)

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

    # Remove timezone and force it to be config timezone
    log(f"Before converting, timestamp_gps is: \n{df['timestamp_gps']}")

    df["timestamp_gps"] = df["content"].apply(
        lambda x: pd.to_datetime(
            pendulum.from_timestamp(safe_cast(x["timestamp_gps"], int, 0))
            .replace(tzinfo=None)
            .set(tz="UTC")
            .isoformat()
        )  # .tz_convert(timezone)
    )

    log(f"After converting the timezone, timestamp_gps is: \n{df['timestamp_gps']}")

    # Filter data for 0 <= time diff <= 1min
    try:
        datahora_col = "timestamp_gps"
        df_treated = df
        try:
            df_treated[datahora_col] = df_treated[datahora_col].apply(
                lambda x: x.tz_convert(timezone)
            )
        except TypeError:
            df_treated[datahora_col] = df_treated[datahora_col].apply(
                lambda x: x.tz_localize(timezone)
            )
        try:
            df_treated["timestamp_captura"] = df_treated["timestamp_captura"].apply(
                lambda x: x.tz_convert(timezone)
            )
        except TypeError:
            df_treated["timestamp_captura"] = df_treated["timestamp_captura"].apply(
                lambda x: x.tz_localize(timezone)
            )
        mask = (df_treated["timestamp_captura"] - df_treated[datahora_col]).apply(
            lambda x: timedelta(seconds=0) <= x <= timedelta(minutes=1)
        )
        df_treated = df_treated[mask]
        log(f"Shape antes da filtragem: {df.shape}")
        log(f"Shape após a filtragem: {df_treated.shape}")
        if df_treated.shape[0] == 0:
            error = ValueError("After filtering, the dataframe is empty!")
            log_critical(f"@here\nFailed to filter BRT data: \n{error}")
        df = df_treated  # pylint: disable=C0103
    except Exception:  # pylint: disable = W0703
        err = traceback.format_exc()
        log_critical(f"@here\nFailed to filter BRT data: \n{err}")
    # log_critical(f"@here\n Got SPPO data at {timestamp} sucessfully")

    return {"data": df, "error": error}
