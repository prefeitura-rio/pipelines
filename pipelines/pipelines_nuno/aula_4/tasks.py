# -*- coding: utf-8 -*-
"""
Tasks para o exercicio de esquenta (SME)
"""
import time

import os
import pandas as pd
import matplotlib.pyplot as plt

from prefect import task
from utils import limpa_numero
from utils import cria_caminho

from pipelines.utils.utils import log

# seta algumas variáveis úteis
# timestampArquivo = time.strftime("%Y%m%d-%H%M%S")
# caminhoSaida = '.\\Saida'

@task
def get_time_stamp():
    return time.strftime("%Y%m%d-%H%M%S")


@task
def get_caminho_saida():
    caminho_saida = '.\\Saida'
    cria_caminho(caminho_saida)
    return caminho_saida



# Objetivo: Buscar um número determinado de linhas da API Randomuser em formato CSV
# Parametro: número de linhas a retornar no dataframe
# Retorno: Pandas dataframe
@task
def get_data(linhas):
    log("Buscando dados da API")
    linhas +=1 #para compensar o header
    url = "https://randomuser.me/api/?format=csv&nat=us,br,ca,fr,au&results="+str(linhas)
    #le da API e coloca na codificação UTF-8
    meu_dataframe = pd.read_csv(url, encoding='utf-8')
    return meu_dataframe

# Objetivo: Escrever arquivo com dados da API Randomuser
# Parametro: Pandas dataframe
# Retorno: nenhum
@task
def escreve_resultado(meu_dataframe,timestamp_arquivo):
    log("Escrevendo no arquivo de saída")
    #grava um arquivo diferente a cada execução
    meu_dataframe.to_csv('.\\Saida\\'+timestamp_arquivo+".csv",sep=";",encoding='ansi')


# Objetivo: Converter numeros de celular
# Parametro: Pandas dataframe
# Retorno: Pandas dataframe
@task
def transforma_celulares(meu_dataframe, coluna):
    log("Transforma números de celular")
    meu_dataframe[coluna] = meu_dataframe[coluna].apply(limpa_numero)
    return meu_dataframe

# Objetivo: Imprimir percentual de pessoas por pais e por genero
# Parametro: Dataframe
# Retorno: nenhum
@task
def grava_estatisticas(meu_dataframe, timestamp_arquivo):
    log("Grava estatísticas no arquivo")

    # calcula as porcentagens e formata a saída
    porcentagem_paises = (
        (
            meu_dataframe["location.country"].value_counts()
            / meu_dataframe["location.country"].count()
        )
        * 100
    ).map("{:,.2f}%".format)
    porcentagem_generos = (
        (meu_dataframe["gender"].value_counts() / meu_dataframe["gender"].count()) * 100
    ).map("{:,.2f}%".format)
    # imprime os resultados no terminal para controle
    print("Distribuicao percentual por pais")
    print(porcentagem_paises)
    print("\nDistribuicao percentual por genero")

    print(porcentagem_generos)
    #grava resultados em arquivo
    arquivo = open('.\\Saida\\'+timestamp_arquivo+'.txt', "a")
    arquivo.write(str(porcentagem_paises))
    arquivo.write(str(porcentagem_generos))
    arquivo.close()

# Objetivo: Exibir grafico de distribuição das idades
# Parametro: Dataframe
# Retorno: nenhum
@task
def plota_idades(meu_dataframe,timestamp_arquivo):
    log("Criando gráfico de idades")
    valores_x = meu_dataframe['dob.age']
    #y = meu_dataframe['dob.age'].value_counts
    plt.hist(valores_x,100)
    plt.savefig('.\\Saida\\'+timestamp_arquivo+".png")

@task
def cria_pasta(caminho_saida):
    log("Cria pasta de sáida, caso ela não exista")
    existe = os.path.exists(caminho_saida)
    if not existe:
        os.makedirs(caminho_saida)
