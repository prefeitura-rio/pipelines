"""
Flows for precipitacao_alertario
"""

from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.cor.alagamentos.precipitacao_alertario.tasks import (slice_data, download,
                                                      tratar_dados, salvar_dados,
                                                      upload_to_gcs)
from pipelines.cor.alagamentos.precipitacao_alertario.schedules import minute_schedule

with Flow('precipitacao_alertario') as flow:
    CURRENT_TIME = Parameter('CURRENT_TIME', default=None)  #or pendulum.now("utc")
    DATASET_ID = Parameter('DATASET_ID', default=None)
    TABLE_ID = Parameter('TABLE_ID', default=None)

    data, hora, minuto = slice_data(current_time=CURRENT_TIME)

    dados = download(data=data)
    dados = tratar_dados(dados=dados, hora=hora)
    path = salvar_dados(dados=dados)
    upload_to_gcs(path=path, dataset_id=DATASET_ID, table_id=TABLE_ID)

# para rodar na cloud
flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
flow.schedule = minute_schedule
