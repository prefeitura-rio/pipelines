# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_onibus_gps
"""

import traceback
from datetime import datetime, timedelta
from typing import Dict, List
import pandas as pd
from prefect import task
import pendulum

# EMD Imports #

from pipelines.utils.utils import log, get_vault_secret

# SMTR Imports #

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.tasks import query_logs, get_current_timestamp

# Tasks #


@task
def create_api_url_onibus_realocacao(
    interval_minutes: int = 10,
    timestamp: datetime = None,
    secret_path: str = constants.GPS_SPPO_REALOCACAO_SECRET_PATH.value,
) -> str:
    """
    start_date: datahora mínima do sinal de GPS avaliado
    end_date: datahora máxima do sinal de GPS avaliado
    """

    # Configura parametros da URL
    date_range = {
        "date_range_start": (timestamp - timedelta(minutes=interval_minutes)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        ),
        "date_range_end": timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    url = "http://ccomobility.com.br/WebServices/Binder/wsconecta/EnvioViagensRetroativasSMTR?"

    headers = get_vault_secret(secret_path)["data"]
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

    error = None

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
        log(f"Data received to treat: \n{df_realocacao[col]}")
        temp_time_col_sec = pd.to_datetime(
            df_realocacao[col], format="%Y-%m-%dT%H:%M:%S", errors="coerce"
        ).dt.tz_localize(tz=constants.TIMEZONE.value)
        temp_time_col_msec = pd.to_datetime(
            df_realocacao[col], format="%Y-%m-%dT%H:%M:%S.%f", errors="coerce"
        ).dt.tz_localize(tz=constants.TIMEZONE.value)

        df_realocacao[col] = temp_time_col_sec.fillna(temp_time_col_msec).dt.strftime(
            "%Y-%m-%d %H:%M:%S%z"
        )

        if df_realocacao[col].isna().sum() > 0:
            error = ValueError("After treating, there is null values!")
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")

        log(f"Treated data: \n{df_realocacao[col]}")

    # Ajusta tempo máximo da realocação
    df_realocacao.loc[
        df_realocacao.dataSaida == "1971-01-01 00:00:00-0300", "dataSaida"
    ] = ""

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

    return {"data": df_realocacao.drop_duplicates(), "error": error}


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
            lambda x: timedelta(seconds=-20) <= x <= timedelta(minutes=time_delay)
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


@task
def get_realocacao_recapture_timestamps(start_date: str, end_date: str) -> List:
    """Get timestamps with no file in realocacao to recapture it in a given date range.

    Args:
        start_date (str): Start date in format YYYY-MM-DD
        end_date (str): End date in format YYYY-MM-DD

    Returns:
        List: List of dicts containing the timestamps to recapture
    """

    log(
        f"Getting timestamps to recapture between {start_date} and {end_date}",
        level="info",
    )

    errors = []
    timestamps = []
    previous_errors = []
    flag_break = False

    dates = pd.date_range(start=start_date, end=end_date)

    dates = dates.strftime("%Y-%m-%d").to_list()

    for date in dates:
        datetime_filter = get_current_timestamp.run(f"{date} 00:00:00")

        if datetime_filter > get_current_timestamp.run():
            flag_break = True
            datetime_filter = get_current_timestamp.run()
            log(
                """Datetime filter is greater than current timestamp,
                   using current timestamp instead""",
                level="warning",
            )

        errors_temp, timestamps_temp, previous_errors_temp = query_logs.run(
            dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
            table_id=constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value,
            datetime_filter=datetime_filter,
            max_recaptures=2 ^ 63 - 1,
            interval_minutes=10,
        )

        if timestamps_temp == []:
            log(
                f"""From {date}, there are no recapture timestamps""",
                level="info",
            )
            continue

        errors = errors + ([errors_temp] * len(timestamps_temp))
        timestamps = timestamps + timestamps_temp
        previous_errors = previous_errors + previous_errors_temp

        if flag_break:
            log(
                "Breaking loop because datetime filter is greater than current timestamp",
                level="warning",
            )
            break

    timestamps = [timestamp.strftime("%Y-%m-%d %H:%M:%S") for timestamp in timestamps]

    log(
        f"""From {start_date} to {end_date}, there are {len(timestamps)} recapture timestamps: \n
            {timestamps}""",
        level="info",
    )

    combined_data = []

    for error, timestamp, previous_error in zip(errors, timestamps, previous_errors):
        data = {
            "error": error,
            "timestamp": timestamp,
            "previous_error": previous_error,
            "recapture": True,
        }

        combined_data.append(data)

    log(
        f"""Combined data: \n
            {combined_data}""",
        level="info",
    )

    return combined_data
