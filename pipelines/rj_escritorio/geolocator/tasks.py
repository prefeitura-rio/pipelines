"""
Tasks for geolocator
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


import time

import basedosdados as bd
import pandas as pd
import prefect
from pipelines.rj_escritorio.geolocator.constants import (
    constants as geolocator_constants,
)
from pipelines.rj_escritorio.geolocator.utils import geolocator
from pipelines.utils.utils import log
from prefect import task


# Importando os dados
@task
def importa_bases_e_chamados() -> list:
    """
    Importa a base de endereços completa e os endereços dos chamados que entraram no dia anterior.
    """
    query = """
    SELECT * FROM `rj-escritorio-dev.seconserva_buracos.habitacao_urbana_enderecos_geolocalizados`
    """
    base_enderecos_atual = bd.read_sql(
        query, billing_project_id="rj-escritorio-dev", from_file=True
    )
    enderecos_conhecidos = base_enderecos_atual["endereco_completo"]

    d1 = prefect.context.get("yesterday")
    d2 = prefect.context.get("today")
    query_2 = f"""
    with teste as (
    SELECT 
    'Brasil' pais,
    'RJ' estado,
    'Rio de Janeiro' municipio,
    no_bairro bairro,
    no_logradouro logradouro,
    ds_endereco_numero numero_porta,
    CONCAT(no_logradouro, ' ', ds_endereco_numero, ', ', no_bairro, ', ', 'Rio de Janeiro, RJ, Brasil') endereco_completo
    FROM `rj-segovi.administracao_servicos_publicos_1746_staging.chamado`
        WHERE no_logradouro IS NOT NULL
        AND dt_inicio >= '{d1}' AND dt_inicio <= '{d2}'
        ORDER BY id_chamado ASC
    )
    select distinct endereco_completo, pais, estado, municipio, bairro, logradouro, numero_porta from teste
    """
    chamados_ontem = bd.read_sql(query_2, billing_project_id="rj-escritorio-dev", from_file=True)
    enderecos_ontem = chamados_ontem["endereco_completo"]

    return [enderecos_conhecidos, enderecos_ontem, chamados_ontem, base_enderecos_atual]


# Pegando apenas os endereços NOVOS que entraram ontem
@task
def enderecos_novos(lista_enderecos: list) -> pd.DataFrame:
    """
    Retorna apenas os endereços não catalogados que entraram no dia anterior
    """
    novos_enderecos = lista_enderecos[1][~lista_enderecos[1].isin(lista_enderecos[0])]
    base_enderecos_novos = lista_enderecos[2][
        lista_enderecos[2]["endereco_completo"].isin(novos_enderecos)
    ]
    # for i in range(0, 4):
    #     log(i)
    #     log(lista_enderecos[i].head(5))
    return base_enderecos_novos


# Geolocalizando
@task
def geolocaliza_enderecos(base_enderecos_novos: pd.DataFrame) -> pd.DataFrame:
    """
    Geolocaliza todos os novos endereços que entraram no dia anterior.
    """
    start_time = time.time()
    coordenadas = base_enderecos_novos["endereco_completo"].apply(
        lambda x: pd.Series(geolocator(x), index=["lat", "long"])
    )

    log(f"--- {(time.time() - start_time)} seconds ---")

    base_enderecos_novos["latitude"] = coordenadas["lat"]
    base_enderecos_novos["longitude"] = coordenadas["long"]

    return base_enderecos_novos


# Adicionando os endereços novos geolocalizados na base de endereços que já possuímos
@task
def cria_csv(base_enderecos_atual, base_enderecos_novos):
    """
    Une os endereços previamente catalogados com os novos e cria um csv.
    """
    base_enderecos_atualizada = base_enderecos_atual.append(base_enderecos_novos, ignore_index=True)
    base_enderecos_atualizada.to_csv(geolocator_constants.PATH_BASE_ENDERECOS.value, index=False)
