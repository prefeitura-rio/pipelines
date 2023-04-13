# -*- coding: utf-8 -*-

# Importando bibliotecas

import requests
import pandas as pd
import matplotlib.pyplot as plt

from prefect import task
from pipelines.utils.utils import log


# *****************************************  Etapa 1  ***********************************
@task
def GetApiRndUser(nreg=10, inc="", gender="", nat=""):
    """
    O que a função faz:
    Lê a API https://randomuser.me com os parâmetros fornecidos e
    exporta o resultado para um arquivo csv.

    Argumentos:
        nreg..: retorna n registros da API 'randomuser.me'. Padrão: 10
        inc...: retorna campos adicionais que são opcionais.
                Os campos padrão são: 'title_name', 'first_name', 'last_name', 'gender', 'date_dob',
                'age_dob', 'country_location', 'state_location', 'city_location', 'phone', 'cell'
        gender: filtra o gênero dos registros a serem trazidos da API. Opções: 'male' ou 'female'.
                Padrão: ambos => 'male' e 'female'
        nat...: filtra o(s) código(s) do(s) país(es) da naturalidade. Opções:
                Padrão: todos.

    Retorna: o dataframe no formato de um arquivo csv

    Exemplo: GetApiRndUser(10, gender='female', inc='nat')"""

    # Montando os campos
    sCampos = "name,gender,dob,location,phone,cell"
    sCamposExtras = inc
    if sCamposExtras != "":
        sCampos = sCampos + "," + sCamposExtras

    # Definindo a string da API
    sAPI = (
        "https://randomuser.me/api/?results="
        + str(nreg)
        + "&inc="
        + sCampos
        + "&noinfo"
    )
    if gender != "":
        sAPI = sAPI + "&gender=" + gender

    # Pegando os dados da API
    response = requests.get(sAPI)

    # Retornando o dicionário
    dicResponse = response.json()

    # Criando um DataFrame a partir do dicionário
    dfResult = pd.DataFrame.from_dict(dicResponse["results"])

    # Transformando o campo "Name" em colunas "flat"
    lstTitleName = [item["title"] for item in dfResult["name"]]
    dfResult["title_name"] = lstTitleName
    lstFirstName = [item["first"] for item in dfResult["name"]]
    dfResult["first_name"] = lstFirstName
    lstLastName = [item["last"] for item in dfResult["name"]]
    dfResult["last_name"] = lstLastName
    dfResult = dfResult.drop("name", axis=1)

    # Transformando o campo "location" em colunas "flat"
    lstCountryLocation = [item["country"] for item in dfResult["location"]]
    dfResult["country_location"] = lstCountryLocation
    lstStateLocation = [item["state"] for item in dfResult["location"]]
    dfResult["state_location"] = lstStateLocation
    lstCityLocation = [item["city"] for item in dfResult["location"]]
    dfResult["city_location"] = lstCityLocation
    dfResult = dfResult.drop("location", axis=1)

    # Transformando o campo "DoB" em colunas "flat"
    lstDateDob = [item["date"] for item in dfResult["dob"]]
    dfResult["date_dob"] = lstDateDob
    dfResult["date_dob"] = pd.to_datetime(dfResult.date_dob)
    dfResult["date_dob"] = dfResult["date_dob"].dt.strftime("%d/%m/%Y")
    lstAgeDob = [item["age"] for item in dfResult["dob"]]
    dfResult["age_dob"] = lstAgeDob
    dfResult = dfResult.drop("dob", axis=1)

    # Arrumando as colunas do DataFrame
    lstColunas = [
        "title_name",
        "first_name",
        "last_name",
        "gender",
        "date_dob",
        "age_dob",
        "country_location",
        "state_location",
        "city_location",
        "phone",
        "cell",
    ]
    if sCamposExtras != "":
        lstColunasExtras = sCamposExtras.split(",")
        lstColunas.extend(lstColunasExtras)

    # Transformando Campos "flat" opcionais

    if "registered" in sCamposExtras:
        lstDateReg = [item["date"] for item in dfResult["registered"]]
        dfResult["date_reg"] = lstDateReg
        dfResult["date_reg"] = pd.to_datetime(dfResult.date_reg)
        dfResult["date_reg"] = dfResult["date_reg"].dt.strftime("%d/%m/%Y")
        lstColunas.append("date_reg")

        lstAgeReg = [item["age"] for item in dfResult["registered"]]
        dfResult["age_reg"] = lstAgeReg
        lstColunas.append("age_reg")

        lstColunas.remove("registered")
        dfResult = dfResult.drop("registered", axis=1)

    # Arrumando as colunas do DataFrame
    dfResult = dfResult[lstColunas]

    # Exportando o DataFrame para CSV
    dfResult.to_csv(r"Resultado.csv", sep=";")

    log("Etapa 1: Dados baixados da API e gravados no formato csv ...")
    return dfResult


# ***************************************************************************************
def FormataTexto(texto, sep="-", pos=3):
    """O que a função faz:
    Formata um texto usando o separador sep na posição pos

    Argumentos:
        texto: texto a ser formatado
        sep: é o tipo de separador a ser usado na formatação. Padrão: '-'
        pos: é a posição em que entrará o separador a partir do final. Padrão: 3

    Retorna:
        o texto formatado

    Obs:a formatação se dará do final para o início, se repetindo até o início
    Exemplo: print(FormataTexto('1234567890', '-', 3)) => retorna: 1-234-567-890"""

    new = ""
    tam = len(texto)
    for i in range(tam):
        resto = (tam - i) % pos
        if (i != 0) and (resto == 0):
            new = new + sep + texto[i]
        else:
            new = new + texto[i]
    return new


# *****************************************  Etapa 2  ***********************************
@task
def FormtaTelefoneDf(dfEntrada, sep="-", pos=3):
    """O que a função faz:
    Recebe um dataframe e o retorna após a formatação das colunas de phone e cell
    de acordo com o separador e posição

    Argumentos:
        sep: é o tipo de separador a ser usado na formatação. Padrão: '-'
        pos: é a posição em que entrará o separador a partir do final. Padrão: 3

    Retorna:
        o dataframe formatado

    Exemplo da chamada: FormtaTelefoneDf(GetApiRndUser(15))"""

    dfSaida = dfEntrada

    # Limpa e formata a coluna de telefone
    dfSaida["phone"] = dfEntrada["phone"].str.replace("(", "")
    dfSaida["phone"] = dfEntrada["phone"].str.replace(")", "")
    dfSaida["phone"] = dfEntrada["phone"].str.replace("-", "")
    dfSaida["phone"] = dfEntrada["phone"].str.replace(" ", "")
    for i, phone in enumerate(dfSaida["phone"]):
        dfSaida["phone"] = dfSaida["phone"].str.replace(phone, FormataTexto(phone))

    # Limpa e formata a coluna de celular
    dfSaida["cell"] = dfEntrada["cell"].str.replace("(", "")
    dfSaida["cell"] = dfEntrada["cell"].str.replace(")", "")
    dfSaida["cell"] = dfEntrada["cell"].str.replace("-", "")
    dfSaida["cell"] = dfEntrada["cell"].str.replace(" ", "")
    for i, cell in enumerate(dfSaida["cell"]):
        dfSaida["cell"] = dfSaida["cell"].str.replace(cell, FormataTexto(cell))

    log("Etapa 2: Dados de telefone e celular formatados...")
    return dfSaida


# *****************************************  Etapa 3  ***********************************
@task
def PlotaGrafico(dfResult):
    """Função para plotar gráficos
    # PlotaGrafico(GetApiRndUser(500))"""

    # Analisando o percentual de usuários por gênero
    estatistica_genero = (
        dfResult["gender"].value_counts(normalize=True).mul(100).round(1).astype(str)
        + "%"
    )

    # Analisando o percentual de usuários por país
    estatistica_pais = (
        dfResult["country_location"]
        .value_counts(normalize=True)
        .mul(100)
        .round(1)
        .astype(str)
        + "%"
    )

    # Gravando dados de estatística de usuários  por gênero no arquivo: "estatistica.txt"
    log("Etapa 3: Gravando dados de usuários  por gênero no arquivo estatistica.txt")

    with open("Estatistica_de_Usuarios_por_Genero_e_Pais.txt", "w") as relatorio:

        relatorio.write("\nEstatística de Usuários por Gênero (Percentual)\n")
        relatorio.write(str(estatistica_genero))
        relatorio.write("\n")

        relatorio.write("\nEstatística de Usuários por País (Percentual)\n")
        relatorio.write(str(estatistica_pais))

        relatorio.close()

    # Analisando a estatística de usuários por idade
    estatistica_idade = dfResult["age_dob"].value_counts()

    # Plotando o Gráfico
    log("Etapa 3: Plotando o gráfico de usuários por idade...")
    ax = estatistica_idade.plot(kind="bar", figsize=(15, 10), legend=True)
    ax.set_title("Estatística de Usuários por Idade", fontsize=14)
    ax.set_xlabel("Idade", fontsize=12)
    ax.set_ylabel("Quantidade de Usuários", fontsize=12)

    plt.savefig(
        "Estatistica_de_Usuarios_por_Idade.pdf", format="pdf", bbox_inches="tight"
    )
    plt.show()

    log("Etapa 3: Gráficos gerados...")
    return


# *****************************************  Etapa 4  ***********************************
@task
def GroupByCountryState(dfResult):
    lstColunasIniciais = ["country_location", "state_location"]
    dfResult = dfResult.sort_values(lstColunasIniciais)
    lstColunas = list(dfResult.columns.values)
    lstColunas.remove("country_location")
    lstColunas.remove("state_location")
    lstColunasIniciais.extend(lstColunas)
    dfResult = dfResult[lstColunasIniciais]
    log("Etapa 4: Dados agrupados...")
    log("Processamento concluído.")
    return dfResult
