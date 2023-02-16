# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Tasks for precipitacao_alertario
"""
from datetime import timedelta
from pathlib import Path
from typing import Union, Tuple

import numpy as np
import pandas as pd
import pendulum
import prefect
from prefect import task, Flow
from prefect.backend.flow_run import FlowRunView, watch_flow_run
from prefect.engine.signals import signal_from_state

import pandas_read_xml as pdx

# from prefect import context

from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.precipitacao_alertario.utils import (
    parse_date_columns,
)
from pipelines.utils.utils import (
    log,
    to_partitions,
    save_updated_rows_on_redis,
)


@task(timeout=60 * 2)  # 2 minutes
def wait_for_flow_run(
    flow_run_id: str,
    stream_states: bool = True,
    stream_logs: bool = False,
    raise_final_state: bool = False,
) -> "FlowRunView":
    """
    Task to wait for a flow run to finish executing, streaming state and log information

    Args:
        - flow_run_id: The flow run id to wait for
        - stream_states: Stream information about the flow run state changes
        - stream_logs: Stream flow run logs; if `stream_state` is `False` this will be
            ignored
        - raise_final_state: If set, the state of this task will be set to the final
            state of the child flow run on completion.

    Returns:
        FlowRunView: A view of the flow run after completion
    """

    flow_run = FlowRunView.from_flow_run_id(flow_run_id)

    for log in watch_flow_run(
        flow_run_id, stream_states=stream_states, stream_logs=stream_logs
    ):
        message = f"Flow {flow_run.name!r}: {log.message}"
        prefect.context.logger.log(log.level, message)

    # Get the final view of the flow run
    flow_run = flow_run.get_latest()

    if raise_final_state:
        state_signal = signal_from_state(flow_run.state)(
            message=f"{flow_run_id} finished in state {flow_run.state}",
            result=flow_run,
        )
        raise state_signal
    else:
        return flow_run


@task(
    nout=2,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def tratar_dados(
    dataset_id: str, table_id: str, mode: str = "dev"
) -> Tuple[pd.DataFrame, bool]:
    """
    Renomeia colunas e filtra dados com a hora e minuto do timestamp
    de execução mais próximo à este
    """

    url = "http://alertario.rio.rj.gov.br/upload/xml/Chuvas.xml"
    dados = pdx.read_xml(url, ["estacoes"])
    dados = pdx.fully_flatten(dados)

    drop_cols = [
        "@hora",
        "estacao|@nome",
        "estacao|@type",
        "estacao|localizacao|@bacia",
        "estacao|localizacao|@latitude",
        "estacao|localizacao|@longitude",
    ]
    rename_cols = {
        "estacao|@id": "id_estacao",
        "estacao|chuvas|@h01": "acumulado_chuva_1_h",
        "estacao|chuvas|@h04": "acumulado_chuva_4_h",
        "estacao|chuvas|@h24": "acumulado_chuva_24_h",
        "estacao|chuvas|@h96": "acumulado_chuva_96_h",
        "estacao|chuvas|@hora": "data_medicao_utc",
        "estacao|chuvas|@m15": "acumulado_chuva_15_min",
        "estacao|chuvas|@mes": "acumulado_chuva_mes",
    }

    dados = pdx.fully_flatten(dados).drop(drop_cols, axis=1).rename(rename_cols, axis=1)
    log(f"\n[DEBUG]: df.head() {dados.head()}")

    # Converte de UTC para horário São Paulo
    dados["data_medicao_utc"] = pd.to_datetime(dados["data_medicao_utc"])
    date_format = "%Y-%m-%d %H:%M:%S"
    dados["data_medicao"] = dados["data_medicao_utc"].dt.strftime(date_format)

    # Alterando valores ND, '-' e np.nan para NULL
    dados.replace(["ND", "-", np.nan], [None, None, None], inplace=True)

    # Converte variáveis que deveriam ser float para float
    float_cols = [
        "acumulado_chuva_15_min",
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
    ]
    dados[float_cols] = dados[float_cols].apply(pd.to_numeric, errors="coerce")

    # Altera valores negativos para None
    dados[float_cols] = np.where(dados[float_cols] < 0, None, dados[float_cols])

    # Elimina linhas em que o id_estacao é igual mantendo a de menor valor nas colunas float
    dados.sort_values(["id_estacao", "data_medicao"] + float_cols, inplace=True)
    dados.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first")

    log(f"uniquesss df >>>, {type(dados.id_estacao.unique()[0])}")
    dados["id_estacao"] = dados["id_estacao"].astype(str)

    dados = save_updated_rows_on_redis(
        dados,
        dataset_id,
        table_id,
        unique_id="id_estacao",
        date_column="data_medicao",
        date_format=date_format,
        mode=mode,
    )

    dados["id_estacao"] = dados["id_estacao"].astype(int)

    # Fixar ordem das colunas
    dados = dados[
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

    # If df is empty stop flow
    empty_data = dados.shape[0] == 0
    log(f"[DEBUG]: dataframe is empty: {empty_data}")

    return dados, empty_data


@task
def salvar_dados(dados: pd.DataFrame) -> Union[str, Path]:
    """
    Salvar dados tratados em csv para conseguir subir pro GCP
    """

    prepath = Path("/tmp/precipitacao_alertario/")
    prepath.mkdir(parents=True, exist_ok=True)

    partition_column = "data_medicao"
    dataframe, partitions = parse_date_columns(dados, partition_column)
    current_time = pendulum.now("America/Sao_Paulo").strftime("%Y%m%d%H%M")

    # Cria partições a partir da data
    to_partitions(
        data=dataframe,
        partition_columns=partitions,
        savepath=prepath,
        data_type="csv",
        suffix=current_time,
    )
    log(f"[DEBUG] Files saved on {prepath}")
    return prepath
