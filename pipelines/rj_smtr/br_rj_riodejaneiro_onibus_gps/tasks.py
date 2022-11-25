# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_onibus_gps
"""

import traceback
from datetime import datetime, timedelta
from typing import Dict
import pandas as pd
from prefect import task
import pendulum

# EMD Imports #

from pipelines.utils.utils import log, get_vault_secret

# SMTR Imports #

from pipelines.rj_smtr.constants import constants

# Tasks #


@task
def create_api_url_onibus_realocacao(
    timestamp: datetime = None,
) -> str:
    """
    start_date: datahora mínima do sinal de GPS avaliado
    end_date: datahora máxima do sinal de GPS avaliado
    """

    # Configura parametros da URL
    date_range = {
        "date_range_start": (
            timestamp
            - timedelta(minutes=constants.GPS_SPPO_REALOCACAO_INTERVAL_MINUTES.value)
        ).strftime("%Y-%m-%dT%H:%M:%S"),
        "date_range_end": timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    url = "http://ccomobility.com.br/WebServices/Binder/wsconecta/EnvioViagensRetroativasSMTR?"

    headers = get_vault_secret(constants.GPS_SPPO_REALOCACAO_SECRET_PATH.value)["data"]
    key = list(headers)[0]
    url = f"{url}{key}={{secret}}"

    url += f"&dataInicial={date_range['date_range_start']}&dataFinal={date_range['date_range_end']}"

    log(f"Request data from URL:\n{url}")
    return url.format(secret=headers[key])


@task
def pre_treatment_br_rj_riodejaneiro_onibus_realocacao(
    status: dict, timestamp: datetime
) -> Dict:
    """Basic data treatment for bus gps relocation data. Converts unix time to datetime,
    and apply filtering to stale data that may populate the API response.

    Args:
        status (dict): dict containing the status of the request made to the
        API. Must contain keys: data and error

    Returns:
        df_realocacao: pandas.core.DataFrame containing the treated data.
    """

    if status["error"] is not None:
        return {"data": pd.DataFrame(), "error": status["error"]}

    if status["data"] == []:
        log("Data is empty, skipping treatment...")
        return {"data": pd.DataFrame(), "error": status["error"]}

    log(f"Data received to treat: \n{status['data'][:5]}")
    df_realocacao = pd.DataFrame(status["data"])  # pylint: disable=c0103
    # df_realocacao["timestamp_captura"] = timestamp
    df_realocacao["timestamp_captura"] = timestamp.isoformat()
    # log(f"Before converting, datahora is: \n{df_realocacao['datahora']}")

    # Ajusta tipos de data
    dt_cols = [
        "dataEntrada",
        "dataOperacao",
        "dataSaida",
        "dataProcessado",
    ]
    for col in dt_cols:
        log(f"Converting column {col}")
        df_realocacao[col] = pd.to_datetime(df_realocacao[col]).dt.tz_localize(
            tz=constants.TIMEZONE.value
        )

    # Ajusta tempo máximo da realocação
    df_realocacao.loc[
        df_realocacao.dataSaida == "1971-01-01 00:00:00", "dataSaida"
    ] = ""

    # TODO: separar os filtros num dicionario

    # Renomeia colunas
    cols = {
        "veiculo": "id_veiculo",
        "dataOperacao": "datetime_operacao",
        "linha": "servico",
        "dataEntrada": "datetime_entrada",
        "dataSaida": "datetime_saida",
        "dataProcessado": "timestamp_processamento",
    }

    df_realocacao = df_realocacao.rename(columns=cols)

    return {"data": df_realocacao.drop_duplicates(), "error": None}


@task
def create_api_url_onibus_gps(version: str, timestamp: datetime = None) -> str:
    """
    Generates the complete URL to get data from API.
    """

    if version == 2:
        url = constants.GPS_SPPO_API_BASE_URL_V2.value
        source = constants.GPS_SPPO_API_SECRET_PATH_V2.value
    if version == 1:
        url = constants.GPS_SPPO_API_BASE_URL.value
        source = constants.GPS_SPPO_API_SECRET_PATH.value

    if not timestamp:
        timestamp = pendulum.now(constants.TIMEZONE.value).replace(
            second=0, microsecond=0
        )

    headers = get_vault_secret(source)["data"]
    key = list(headers)[0]
    url = f"{url}{key}={{secret}}"

    if source == "sppo_api_v2":
        date_range = {
            "start": (timestamp - timedelta(minutes=6)).strftime("%Y-%m-%d+%H:%M:%S"),
            "end": (timestamp - timedelta(minutes=5)).strftime("%Y-%m-%d+%H:%M:%S"),
        }
        url += f"&dataInicial={date_range['start']}&dataFinal={date_range['end']}"

    log(f"Request data from URL: {url}")
    return url.format(secret=headers[key])


@task
# pylint: disable=too-many-locals
def pre_treatment_br_rj_riodejaneiro_onibus_gps(
    status: dict, timestamp: datetime, version: int = 1, recapture: bool = False
) -> Dict:
    """Basic data treatment for bus gps data. Converts unix time to datetime,
    and apply filtering to stale data that may populate the API response.

    Args:
        status_dict (dict): dict containing the status of the request made to the
        API. Must contain keys: data and error
        version (int, optional): Source API version. Temporary argument
        for testing
        timestamp (str): Capture data timestamp.

    Returns:
        df_gps: pandas.core.DataFrame containing the treated data.
    """
    if status["error"] is not None:
        return {"data": pd.DataFrame(), "error": status["error"]}

    if status["data"] == []:
        log("Data is empty, skipping treatment...")
        return {"data": pd.DataFrame(), "error": status["error"]}

    error = None
    timezone = constants.TIMEZONE.value

    log(f"Data received to treat: \n{status['data'][:5]}")
    df_gps = pd.DataFrame(status["data"])  # pylint: disable=c0103
    df_gps["timestamp_captura"] = timestamp
    log(f"Before converting, datahora is: \n{df_gps['datahora']}")

    # Remove timezone and force it to be config timezone
    if version == 1:
        timestamp_cols = ["datahora"]
    elif version == 2:
        timestamp_cols = ["datahora", "datahoraenvio"]
    if recapture:
        timestamp_cols.append("datahoraservidor")
    for col in timestamp_cols:
        print(f"Before converting, {col} is: \n{df_gps[col].head()}")  # log
        df_gps[col] = (
            pd.to_datetime(df_gps[col].astype(float), unit="ms")
            .dt.tz_localize(tz="UTC")
            .dt.tz_convert(timezone)
        )
        log(f"After converting the timezone, {col} is: \n{df_gps[col].head()}")

    # Filter data
    try:
        log(f"Shape before filtering: {df_gps.shape}")
        if version == 1:
            filter_col = "timestamp_captura"
            time_delay = constants.GPS_SPPO_CAPTURE_DELAY_V1.value
        elif version == 2:
            filter_col = "datahoraenvio"
            time_delay = constants.GPS_SPPO_CAPTURE_DELAY_V2.value
        if recapture:
            server_mask = (
                df_gps["datahoraenvio"] - df_gps["datahoraservidor"]
            ) <= timedelta(minutes=constants.GPS_SPPO_RECAPTURE_DELAY_V2.value)
            df_gps = df_gps[server_mask]  # pylint: disable=c0103

        mask = (df_gps[filter_col] - df_gps["datahora"]).apply(
            lambda x: timedelta(seconds=0) <= x <= timedelta(minutes=time_delay)
        )

        # Select and drop duplicated data
        cols = [
            "ordem",
            "latitude",
            "longitude",
            "datahora",
            "velocidade",
            "linha",
            "timestamp_captura",
        ]
        df_gps = df_gps[mask][cols]  # pylint: disable=c0103
        df_gps = df_gps.drop_duplicates(  # pylint: disable=c0103
            ["ordem", "latitude", "longitude", "datahora", "timestamp_captura"]
        )

        log(f"Shape after filtering: {df_gps.shape}")
        if df_gps.shape[0] == 0:
            error = ValueError("After filtering, the dataframe is empty!")
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")
    except Exception:  # pylint: disable = W0703
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return {"data": df_gps, "error": error}
