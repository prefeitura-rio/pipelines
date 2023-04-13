# -*- coding: utf-8 -*-
from prefect import task
from pipelines.utils.utils import log
import matplotlib.pyplot
import os
import pandas
import re
import requests


@task
def obter_dados(n: int) -> dict:
    log("Obtendo dados...")

    # Efetuar requisição da API.
    response = requests.get(f"https://randomuser.me/api/?results={n}")
    if response.status_code != 200:
        log("API retornou status", response.status_code)
        return None

    # Verificar se algum erro foi retornado.
    dados = response.json()
    if "error" in dados:
        log("API retornou erro:", dados["error"])
        return None

    return dados


@task
def converter_dados(dados: dict) -> pandas.DataFrame:
    log("Convertendo dados...")

    # Normalizar dados para torná-los planos.
    normalizado = pandas.json_normalize(dados["results"])

    # Converter dados em DataFrame.
    frame = pandas.DataFrame(normalizado)

    # Salvar dados em CSV.
    frame.to_csv("dados.csv")

    return frame


@task
def converter_telefone(frame_in: pandas.DataFrame) -> pandas.DataFrame:
    log("Convertendo telefones...")

    # Copiar DataFrame de entrada.
    frame_out = frame_in.copy()

    # Transformar números de telefone e celular.
    for nome_series in ("phone", "cell"):
        # Transformar série e substituir a existente.
        # A função lambda retira caracteres adicionais do número e deixa somente os dígitos.
        frame_out[nome_series] = frame_out[nome_series].transform(
            lambda tel: re.sub(r"""[^0-9]""", "", tel)
        )

    return frame_out


@task
def gerar_relatorio_grafico(
    frame: pandas.DataFrame, report: bool, idades: bool
) -> None:
    if report:
        log("Criando relatório...")

        # Iniciar relatório em texto.
        f = open("relatorio.txt", "w")

        # Adicionar parâmetros definidos ao relatório.
        for series in report:
            f.write("{0}:\n".format(series))
            for value, count in frame[series].value_counts().items():
                f.write(
                    "- {0}: {1:.01f}%\n".format(
                        value, (count / len(frame[series])) * 100
                    )
                )
            f.write("\n")

        # Finalizar relatório.
        f.close()

    if idades:
        log("Criando gráfico...")

        # Gerar gráfico de distribuição de idades.
        matplotlib.pyplot.title("Distribuição de Idades")
        matplotlib.pyplot.hist(frame["dob.age"], bins=idades)

        # Salvar gráfico em arquivo.
        matplotlib.pyplot.savefig("idades.png")


@task
def agrupar(frame: pandas.DataFrame, series: str) -> pandas.DataFrame:
    log("Agrupando dados...")

    # Ordenar DataFrame pelos parâmetros definidos.
    frame = frame.sort_values(series)

    # Salvar novos dados em CSV.
    frame.to_csv("dados.csv")

    return frame


@task
def particionar_dados(frame: pandas.DataFrame, series: str) -> None:
    log("Particionando dados...")

    # Efetuar particionamento por séries.
    for group in frame.groupby(series).groups:
        # Converter valor para tuple de 1 elemento se somente uma série for particionada.
        if type(group) != tuple:
            group = (group,)

        # Criar DataFrame particionado série por série, montando o caminho da pasta.
        frame_part = frame
        caminho = "partitions"
        for i in range(len(group)):
            # Filtrar DataFrame por valor desta série.
            frame_part = frame_part[frame_part[series[i]] == group[i]]

            # Adicionar partição ao caminho.
            caminho = os.path.join(caminho, f"{series[i]}={group[i]}")

        # Criar pasta para este conjunto de partições caso necessário.
        if not os.path.isdir(caminho):
            os.makedirs(caminho)

        # Salvar arquivo particionado.
        caminho = os.path.join(caminho, "data.csv")
        frame_part.to_csv(caminho)
