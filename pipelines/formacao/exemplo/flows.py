# -*- coding: utf-8 -*-
"""
Example flow
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.formacao.exemplo.tasks import download_data, parse_data, save_report
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import create_table_and_upload_to_gcs

with Flow("EMD: formacao - Exemplo de flow do Prefect CHLL") as formacao_exemplo_flow:
    # Parâmetros
    n_users = Parameter("n_users", default=10)

    # Tasks
    data = download_data(n_users)
    dataframe = parse_data(data)
    path = save_report(dataframe)
    create_table_and_upload_to_gcs(
        data_path=path,
        dataset_id="teste_claurenti",
        table_id="teste_claurenti",
        dump_mode="overwrite",
    )

formacao_exemplo_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
formacao_exemplo_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
formacao_exemplo_flow.schedule = None
