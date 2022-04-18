# -*- coding: utf-8 -*-
# flake8: noqa: E722
"""
Tasks for geolocator
"""

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
    SELECT * FROM `rj-escritorio-dev.dados_mestres_staging.enderecos_geolocalizados`
    """
    base_enderecos_atual = bd.read_sql(
        query, billing_project_id="rj-escritorio-dev", from_file=True
    )
    enderecos_conhecidos = base_enderecos_atual["endereco_completo"]

    d1 = prefect.context.get("yesterday")  # pylint: disable=invalid-name
    d2 = prefect.context.get("today")  # pylint: disable=invalid-name
    query_2 = f"""
    with teste as (
    SELECT
    'Brasil' pais,
    'RJ' estado,
    'Rio de Janeiro' municipio,
    no_bairro bairro,
    id_logradouro,
    no_logradouro logradouro,
    ds_endereco_numero numero_porta,
    CONCAT(no_logradouro, ' ', ds_endereco_numero, ', ', no_bairro,
         ', ', 'Rio de Janeiro, RJ, Brasil') endereco_completo
    FROM `rj-segovi.administracao_servicos_publicos_1746_staging.chamado`
        WHERE no_logradouro IS NOT NULL
        AND dt_inicio >= '{d1}' AND dt_inicio <= '{d2}'
        ORDER BY id_chamado ASC
    )
    select distinct
        endereco_completo, pais, estado, municipio,
        bairro, id_logradouro, logradouro, numero_porta
        from teste
    """
    chamados_ontem = bd.read_sql(
        query_2, billing_project_id="rj-escritorio-dev", from_file=True
    )
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

    try:
        base_enderecos_novos["latitude"] = coordenadas["lat"]
        base_enderecos_novos["longitude"] = coordenadas["long"]
    except:  # pylint: disable=bare-except
        log("Não foram identificados chamados abertos no dia anterior.")

    return base_enderecos_novos


# @task
# def get_today() -> str:
#     """
#     Pega o endereço de hoje no formato YYYY-MM-DD
#     """
#     today = prefect.context.get("today")
#     return today


# Adicionando os endereços novos geolocalizados na base de endereços que já possuímos
@task
def cria_csv(base_enderecos_atual: pd.DataFrame, base_enderecos_novos: pd.DataFrame):
    """
    Une os endereços previamente catalogados com os novos e cria um csv.
    """
    # today = prefect.context.get("today")

    base_enderecos_atualizada = base_enderecos_atual.append(
        base_enderecos_novos, ignore_index=True
    )
    base_enderecos_atualizada.to_csv(
        # f"{geolocator_constants.PATH_BASE_ENDERECOS.value}_{today}.csv", index=False
        geolocator_constants.PATH_BASE_ENDERECOS.value,
        index=False,
    )
