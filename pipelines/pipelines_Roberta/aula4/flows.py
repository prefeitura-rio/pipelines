# -*- coding: utf-8 -*-
"""
Tasks for the example flow
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.decorators import Flow


from pipelines.pipelines_Roberta.aula4.tasks import (
    download_data,
    parse_data,
    save_report,
    format_phone,
    create_reports,
    create_vision_country_state,
)

with Flow("EMD: formacao - flow da aula 4 da Roberta") as formacao_flow_aula4:

    # Parâmetros
    n_users = Parameter("n_users", default=10)

    # Tasks
    data = download_data(n_users)
    dataframe = parse_data(data)
    save_report(dataframe)
    dataframe = format_phone(dataframe)
    create_reports(dataframe)
    dataframe2 = create_vision_country_state(dataframe)

formacao_flow_aula4.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
formacao_flow_aula4.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
formacao_exemplo_flow.schedule = None
