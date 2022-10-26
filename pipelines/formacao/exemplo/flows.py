# -*- coding: utf-8 -*-
"""
    Tasks for example flow
"""

from prefect import Parameter

# <--- Importa o KubernetsRun indincando que o flow será executado em um cluster kubernets (onde nosso os agents estão rodando)
from prefect.run_configs import KubernetsRun

# <--- Importa o GCS para indicar que o flow será armazenado no Google Cloud Storage
from prefect.storage import GCS

from pipelines.constants import constants  # <--- Importa o arquivo de constantes
from pipelines.formacao.exemplo.tasks import (
    download_data,
    format_phone_number,
    gerar_df,
    format_phone_number
)

from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import create_table_and_upload_to_gcs

with Flow(
    "EMD: formacao - Exemplo de flow do Prefect da Stella"
) as formacao_example_flow:
    # Definindo os parâmetros do flow
    n_users = Parameter("n_users", default=10)

    # Tasks
    data = download_data(n_users)
    dataframe = gerar_df(data)
    path = format_phone_number(dataframe, "phone")
    create_table_and_upload_to_gcs(
        data_path=path,
        dataset_id="teste_stella",
        table_id="teste_stella",
        dump_mode="overwrite",
    )

formacao_example_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
formacao_example_flow.run_config = KubernetsRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
formacao_example_flow.schedule = None
