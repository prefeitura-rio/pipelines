# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio.formacao_ricardo_banderia.tasks import (
    df_groupby,
    get_dataframe,
    save_dataframe,
    df_group,
    get_dic_dados,
    get_df,
    report_from_df,
)

# from pipelines.rj_escritorio.formacao_infra.tasks import (
#     hello_name,
# )


from pipelines.utils.decorators import Flow

with Flow(
    "EMD: formacao - Exemplo infra Ricardo Bandeira",
    code_owners=[
        "gabriel",
        "diego",
    ],
) as rj_escritorio_formacao_ricardo_bandeira:

    # Etapa 1: Entendendo os dados
    url = "https://randomuser.me/api/"
    get_dic_dados(url=url)

    # Etapa 2: Coletando dados
    # define os parametros
    parameters = {"gender": "female"}

    # obtém o dataframe
    df = get_dataframe(url=url, qtd=5, parameters=parameters)

    # salva o df em arquivo csv
    # define caminho do arquivo a ser gravado
    file_name = "dados/out.csv"
    save_dataframe(df=df, file_name=file_name)

    # Etapa 4: Analisando dados sem agrupamento
    # obtem o dataframe
    dp = get_df(url_api=url, qtd=10)
    # cria o report a partir do dataframe
    rtrn = report_from_df(df=dp)

    # Etapa 5: Analisando dados com agrupamento
    query = "results=10&inc=location"
    group = "location.country", "location.state"
    df_groupby(url=url, query=query, group=group)


rj_escritorio_formacao_ricardo_bandeira.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio_formacao_ricardo_bandeira.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
