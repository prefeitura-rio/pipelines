# -*- coding: utf-8 -*-
"""
Tasks para o exercicio de esquenta (SME)
"""

import pandas as pd
import time
#import re
import matplotlib.pyplot as plt
#import numpy as np
import os
from prefect import task
#import requests

from pipelines.utils.utils import log
#from utils import log
from utils import limpaNumero
from utils import criaCaminho

#seta algumas variáveis úteis
#timestampArquivo = time.strftime("%Y%m%d-%H%M%S")
#caminhoSaida = '.\\Saida'

@task
def getTimestamp():
    return time.strftime("%Y%m%d-%H%M%S")

@task
def getCaminhoSaida():
    caminhoSaida = '.\\Saida'
    criaCaminho(caminhoSaida)
    return caminhoSaida

#Objetivo: Buscar um número determinado de linhas da API Randomuser em formato CSV
#Parametro: número de linhas a retornar no dataframe
#Retorno: Pandas dataframe
@task
def getData(linhas):
    log("Buscando dados da API")
    linhas +=1 #para compensar o header
    url = "https://randomuser.me/api/?format=csv&nat=us,br,ca,fr,au&results="+str(linhas)
    #le da API e coloca na codificação UTF-8
    meuDataframe = pd.read_csv(url, encoding='utf-8')
    return meuDataframe

#Objetivo: Escrever arquivo com dados da API Randomuser
#Parametro: Pandas dataframe
#Retorno: nenhum
@task
def escreveResultado(meuDataframe,timestampArquivo):
    log("Escrevendo no arquivo de saída")
    #grava um arquivo diferente a cada execução
    meuDataframe.to_csv('.\\Saida\\'+timestampArquivo+".csv",sep=";",encoding='ansi')

#Objetivo: Converter numeros de celular
#Parametro: Pandas dataframe
#Retorno: Pandas dataframe
@task
def transformaCelulares(meuDataframe, coluna):
    log("Transforma números de celular")
    meuDataframe[coluna] = meuDataframe[coluna].apply(limpaNumero)
    return meuDataframe

#Objetivo: Imprimir percentual de pessoas por pais e por genero
#Parametro: Dataframe
#Retorno: nenhum
@task
def gravaEstatisticas(meuDataframe, timestampArquivo):
    log("Grava estatísticas no arquivo")
    #calcula as porcentagens e formata a saída
    porcentagemPaises = ((meuDataframe['location.country'].value_counts()/meuDataframe['location.country'].count())*100).map('{:,.2f}%'.format)
    porcentagemGeneros = ((meuDataframe['gender'].value_counts()/meuDataframe['gender'].count())*100).map('{:,.2f}%'.format)
    #imprime os resultados no terminal para controle
    print("Distribuicao percentual por pais")
    print(porcentagemPaises)
    print("\nDistribuicao percentual por genero")
    print(porcentagemGeneros)
    #grava resultados em arquivo
    f = open('.\\Saida\\'+timestampArquivo+'.txt', "a")
    f.write(str(porcentagemPaises))
    f.write(str(porcentagemGeneros))
    f.close()

#Objetivo: Exibir grafico de distribuição das idades
#Parametro: Dataframe
#Retorno: nenhum
@task
def plotaIdades(meuDataframe,timestampArquivo):
    log("Criando gráfico de idades")
    x = meuDataframe['dob.age']
    y = meuDataframe['dob.age'].value_counts
    plt.hist(x,100)
    plt.savefig('.\\Saida\\'+timestampArquivo+".png")

@task
def criaPasta(caminhoSaida):
    log("Cria pasta de sáida, caso ela não exista")
    existe = os.path.exists(caminhoSaida)
    if not existe:
        os.makedirs(caminhoSaida)