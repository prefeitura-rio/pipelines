# -*- coding: utf-8 -*-
# pylint: disable=C0103,R0914
"""
Tasks for precipitacao_alertario
"""
from datetime import timedelta
from pathlib import Path
from typing import Union, Tuple
import requests

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import pendulum
from prefect import task

from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.precipitacao_alertario.utils import (
    parse_date_columns_old_api,
    treat_date_col,
)
from pipelines.utils.utils import (
    build_redis_key,
    compare_dates_between_tables_redis,
    get_redis_output,
    get_vault_secret,
    log,
    to_partitions,
    parse_date_columns,
    save_str_on_redis,
    save_updated_rows_on_redis,
)


@task(
    nout=2,
    max_retries=10,  # constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_data() -> pd.DataFrame:
    """
    Request data from API and return each data in a different dataframe.
    """

    dicionario = get_vault_secret("alertario_api")
    url = dicionario["data"]["url"]

    try:
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            # Get all tables from HTML structure
            tables = soup.find_all("table")

            # Data cames in Brazillian format and has some extra newline
            tables = [
                str(table).replace(",", ".").replace("\n", "") for table in tables
            ]

            # Convert HTML table to pandas dataframe
            dfr = pd.read_html(str(tables), decimal=",")
        else:
            log(
                f"Erro ao fazer a solicitação. Código de status: {response.status_code}"
            )

    except requests.RequestException as e:
        log(f"Erro durante a solicitação: {e}")

    dfr_pluviometric = dfr[0]
    dfr_meteorological = dfr[1]
    # dfr_rain_conditions = dfr[2]
    # dfr_landslide_probability = dfr[3]

    log(f"\nPluviometric df {dfr_pluviometric.iloc[0]}")
    log(f"\nMeteorological df {dfr_meteorological.iloc[0]}")

    return (
        dfr_pluviometric,
        dfr_meteorological,
    )  # , dfr_rain_conditions, dfr_landslide_probability


@task(
    nout=2,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def treat_pluviometer_and_meteorological_data(
    dfr: pd.DataFrame, dataset_id: str, table_id: str, mode: str = "dev"
) -> Tuple[pd.DataFrame, bool]:
    """
    Renomeia colunas e filtra dados com a hora e minuto do timestamp
    de execução mais próximo à este
    """

    # Treat dfr if is from pluviometers
    if isinstance(dfr.columns, pd.MultiIndex):
        # Keeping only the rightmost level of the MultiIndex columns
        dfr.columns = dfr.columns.droplevel(level=0)

        rename_cols = {
            "N°": "id_estacao",
            "Hora Leitura": "data_medicao",
            "05 min": "acumulado_chuva_5min",
            "10 min": "acumulado_chuva_10min",
            "15 min": "acumulado_chuva_15min",
            "30 min": "acumulado_chuva_30min",
            "1h": "acumulado_chuva_1h",
            "2h": "acumulado_chuva_2h",
            "3h": "acumulado_chuva_3h",
            "4h": "acumulado_chuva_4h",
            "6h": "acumulado_chuva_6h",
            "12h": "acumulado_chuva_12h",
            "24h": "acumulado_chuva_24h",
            "96h": "acumulado_chuva_96h",
            "No Mês": "acumulado_chuva_mes",
        }

    else:
        rename_cols = {
            "N°": "id_estacao",
            "Hora Leitura": "data_medicao",
            "Temp. (°C)": "temperatura",
            "Umi. do Ar (%)": "umidade_ar",
            "Sen. Térmica (°C)": "sensacao_termica",
            "P. Atm. (hPa)": "pressao_atmosferica",
            "P. de Orvalho (°C)": "temperatura_orvalho",
            "Vel. do Vento (Km/h)": "velocidade_vento",
            "Dir. do Vento (°)": "direcao_vento",
        }  # confirmar nome das colunas com inmet

    dfr.rename(columns=rename_cols, inplace=True)

    keep_cols = list(rename_cols.values())

    # Elimina linhas em que o id_estacao é igual mantendo a de menor valor nas colunas float
    # dfr.sort_values(["id_estacao", "data_medicao"] + float_cols, inplace=True)
    dfr.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first")

    dfr["data_medicao"] = pd.to_datetime(
        dfr["data_medicao"], format="%d/%m/%Y - %H:%M:%S"
    )

    log(f"Dataframe before comparing with last data saved on redis {dfr.head()}")
    log(f"Dataframe before comparing with last data saved on redis {dfr.iloc[0]}")

    dfr = save_updated_rows_on_redis(
        dfr,
        dataset_id,
        table_id,
        unique_id="id_estacao",
        date_column="data_medicao",
        date_format="%Y-%m-%d %H:%M:%S",
        mode=mode,
    )

    empty_data = dfr.shape[0] == 0

    if not empty_data:
        see_cols = ["id_estacao", "data_medicao", "last_update"]
        log(
            f"Dataframe after comparing with last data saved on redis {dfr[see_cols].head()}"
        )
        log(f"Dataframe first row after comparing {dfr.iloc[0]}")
        dfr["data_medicao"] = dfr["data_medicao"].dt.strftime("%Y-%m-%d %H:%M:%S")
        log(f"Dataframe after converting to string {dfr[see_cols].head()}")

        # Save max date on redis to compare this with last dbt run
        max_date = str(dfr["data_medicao"].max())
        redis_key = build_redis_key(dataset_id, table_id, name="last_update", mode=mode)
        log(f"Dataframe is not empty. Redis key: {redis_key} and new date: {max_date}")
        save_str_on_redis(redis_key, "date", max_date)

        if not empty_data:
            # Changin values "ND" and "-" to "None"
            dfr.replace(["ND", "-"], [None, None], inplace=True)

        # Fix columns order
        dfr = dfr[keep_cols]
    else:
        # If df is empty stop flow on flows.py
        log("Dataframe is empty. Skipping update flow.")

    return dfr, empty_data


@task
def save_data(
    dfr: pd.DataFrame,
    data_name: str = "temp",
    wait=None,  # pylint: disable=unused-argument
) -> Union[str, Path]:
    """
    Salvar dfr tratados em csv para conseguir subir pro GCP
    """

    prepath = Path(f"/tmp/precipitacao_alertario/{data_name}")
    prepath.mkdir(parents=True, exist_ok=True)

    partition_column = "data_medicao"
    log(f"Dataframe before partitions {dfr.iloc[0]}")
    log(f"Dataframe before partitions {dfr.dtypes}")
    dataframe, partitions = parse_date_columns(dfr, partition_column)
    current_time = pendulum.now("America/Sao_Paulo").strftime("%Y%m%d%H%M")
    log(f"Dataframe after partitions {dataframe.iloc[0]}")
    log(f"Dataframe after partitions {dataframe.dtypes}")

    to_partitions(
        data=dataframe,
        partition_columns=partitions,
        savepath=prepath,
        data_type="csv",
        suffix=current_time,
    )
    log(f"Files saved on {prepath}")
    return prepath


@task
def save_last_dbt_update(
    dataset_id: str,
    table_id: str,
    mode: str = "dev",
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """
    Save on dbt last timestamp where it was updated
    """
    last_update_key = build_redis_key(
        dataset_id, table_id, name="last_update", mode=mode
    )
    last_update = get_redis_output(last_update_key)
    redis_key = build_redis_key(dataset_id, table_id, name="dbt_last_update", mode=mode)
    log(f"Saving {last_update} as last time dbt was updated")
    save_str_on_redis(redis_key, "date", last_update["date"])


@task(skip_on_upstream_skip=False)
def check_to_run_dbt(
    dataset_id: str,
    table_id: str,
    mode: str = "dev",
) -> bool:
    """
    It will run even if its upstream tasks skip.
    """

    key_table_1 = build_redis_key(
        dataset_id, table_id, name="dbt_last_update", mode=mode
    )
    key_table_2 = build_redis_key(dataset_id, table_id, name="last_update", mode=mode)

    format_date_table_1 = "YYYY-MM-DD HH:mm:SS"
    format_date_table_2 = "YYYY-MM-DD HH:mm:SS"

    # Returns true if date saved on table_2 (alertario) is bigger than
    # the date saved on table_1 (dbt).
    run_dbt = compare_dates_between_tables_redis(
        key_table_1, format_date_table_1, key_table_2, format_date_table_2
    )
    log(f">>>> debug data alertario > data dbt: {run_dbt}")
    return run_dbt


@task(
    nout=2,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def treat_old_pluviometer(
    dfr: pd.DataFrame,
    wait=None,  # pylint: disable=unused-argument
) -> pd.DataFrame:
    """
    Renomeia colunas no estilo do antigo flow.
    """

    log(
        f"Starting treating pluviometer data to match old API.\
            Pluviometer table enter as\n{dfr.iloc[0]}"
    )
    rename_cols = {
        "acumulado_chuva_15min": "acumulado_chuva_15_min",
        "acumulado_chuva_1h": "acumulado_chuva_1_h",
        "acumulado_chuva_4h": "acumulado_chuva_4_h",
        "acumulado_chuva_24h": "acumulado_chuva_24_h",
        "acumulado_chuva_96h": "acumulado_chuva_96_h",
    }

    dfr.rename(columns=rename_cols, inplace=True)

    # date_format = "%Y-%m-%d %H:%M:%S"
    # dfr["data_medicao"] = dfr["data_medicao"].dt.strftime(date_format)
    dfr.data_medicao = dfr.data_medicao.apply(treat_date_col)

    # Converte variáveis que deveriam ser float para float
    float_cols = [
        "acumulado_chuva_15_min",
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
    ]
    dfr[float_cols] = dfr[float_cols].apply(pd.to_numeric, errors="coerce")

    # Altera valores negativos para None
    dfr[float_cols] = np.where(dfr[float_cols] < 0, None, dfr[float_cols])

    # Elimina linhas em que o id_estacao é igual mantendo a de menor valor nas colunas float
    dfr.sort_values(["id_estacao", "data_medicao"] + float_cols, inplace=True)
    dfr.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first")

    # Fix columns order
    dfr = dfr[
        [
            "data_medicao",
            "id_estacao",
            "acumulado_chuva_15_min",
            "acumulado_chuva_1_h",
            "acumulado_chuva_4_h",
            "acumulado_chuva_24_h",
            "acumulado_chuva_96_h",
        ]
    ]
    log(
        f"Ending treating pluviometer data to match old API.\
            Pluviometer table finished as\n{dfr.iloc[0]}"
    )
    return dfr


@task
def save_data_old(
    dfr: pd.DataFrame,
    data_name: str = "temp",
    wait=None,  # pylint: disable=unused-argument
) -> Union[str, Path]:
    """
    Salvar dfr tratados em csv para conseguir subir pro GCP
    """

    prepath = Path(f"/tmp/precipitacao_alertario/{data_name}")
    prepath.mkdir(parents=True, exist_ok=True)

    log(f"Dataframe before partitions old api {dfr.iloc[0]}")
    partition_column = "data_medicao"
    dataframe, partitions = parse_date_columns_old_api(dfr, partition_column)
    current_time = pendulum.now("America/Sao_Paulo").strftime("%Y%m%d%H%M")
    log(f"Dataframe after partitions old api {dataframe.iloc[0]}")

    to_partitions(
        data=dataframe,
        partition_columns=partitions,
        savepath=prepath,
        data_type="csv",
        suffix=current_time,
    )
    log(f"{data_name} files saved on {prepath}")
    return prepath
