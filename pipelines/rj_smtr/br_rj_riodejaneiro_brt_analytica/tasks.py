# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_brt_analytica
"""

###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################

from prefect import task
import os
from pipelines.rj_smtr.br_rj_riodejaneiro_brt_analytica.utils import (
    get_intervalos,
    get_day_hour_index,
    fill_hours,
)
from pipelines.rj_smtr.br_rj_riodejaneiro_brt_analytica.constants import constants


@task
def save_model():
    print("Iniciando o processamento do modelo...")
    df_intervalos = get_intervalos()

    modelo = (
        df_intervalos.groupby(
            ["stop_id_origem", "stop_id_destino", "dia_da_semana", "hora"]
        )["delta_tempo"]
        .median()
        .sort_index()
    )

    day_hour_index = get_day_hour_index()
    modelo_filled = (
        modelo.reset_index()
        .groupby(["stop_id_origem", "stop_id_destino"])
        .apply(fill_hours, ["dia_da_semana", "hora"], day_hour_index)
        .reset_index(["stop_id_origem", "stop_id_destino"])
    )

    modelo_filled["delta_tempo_minuto"] = modelo_filled.delta_tempo.dt.seconds.div(60.0)

    path = os.path.join(constants.CURRENT_DIR, "Modelos/Mediana/geral.parquet")
    modelo_filled.to_parquet(path, engine="fastparquet")

    print("Modelo de Mediana salvo.")
