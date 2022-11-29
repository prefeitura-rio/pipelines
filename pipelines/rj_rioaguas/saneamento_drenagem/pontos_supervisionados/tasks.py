# -*- coding: utf-8 -*-
"""
Tasks para pipeline do shapefile localizado no portal do SIURB.
Fonte: Rio-águas.
"""
# pylint: disable= C0327

from pathlib import Path
from typing import Union

import pandas as pd
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from prefect import task

from pipelines.utils.utils import get_vault_secret, log


@task
def wfs_to_df(url_fl: str, url_gis: str) -> pd.DataFrame:
    """
    Função para extrair spatial dataframe a partir de um feature service (WFS) do portal do SIURB.

    Args:
    url_fl (str): URL do feature service (WFS). Exemplo:
    'https://pgeo3.rio.rj.gov.br/arcgis/rest/services/Hosted
    /PARA_COR_COMPLETA_Com_lat_long/FeatureServer/0'
    url_gis (str): URL da HOME do portal do ArcGIS.
    """

    dicionario = get_vault_secret("siurb")
    user = dicionario["data"]["user"]
    pwd = dicionario["data"]["password"]

    GIS(url_gis, user, pwd)
    flayer = FeatureLayer(url_fl)
    sdf = pd.DataFrame.spatial.from_layer(flayer)

    return sdf


@task
def tratar_dados(sdf: pd.DataFrame) -> pd.DataFrame:
    """
    Função para remover, renomear e reordenar colunas do Spatial DataFrame.

    Args:
    sdf (pd.DataFrame): DataFrame com os dados.
    """
    # Remover colunas que iniciam com field_
    # e outros
    col_drop = ['bolsoes_co', 'fid', 'lat', 'long'] + [
        col for col in list(sdf) if col.startswith("field")]
    sdf_drop = sdf.drop(columns=col_drop)

    # Renomear colunas
    col_rename = {"descricao_": "descricao_cor",
                  "informacoe": "infomacoes",
                  "globalid": "id_global"}
    sdf_rename = sdf_drop.rename(col_rename, axis=1)

    # Arrumar ordem das colunas
    col_order = ['codigo', 'classe', 'top_50', 'descricao_cor', 'infomacoes',
                 'eliminado', 'utm_x', 'utm_y', 'ap', 'ra', 'bairro', 'bacia',
                 'sub_bacia', 'medida_cor', 'projeto', 'globalid', 'SHAPE']
    sdf_order = sdf_rename[col_order]

    # Padronizando dados nulos
    # sdf_final = sdf_order.replace("_", None) NAO FUNCIONA
    return sdf_order


@task
def salvar_dados(dados: pd.DataFrame) -> Union[str, Path]:
    """
    Salvar dados em csv.
    """
    base_path = Path("/tmp/pontos_supervisionados/")
    base_path.mkdir(parents=True, exist_ok=True)

    filename = base_path / "pontos_supervisionados.csv"
    log(f"Saving {filename}")
    dados.to_csv(filename, index=False)
    return base_path
