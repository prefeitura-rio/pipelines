"""
Flows for precipitacao_alertario
"""
import pendulum
from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.precipitacao_alertario.tasks import (slice_data, download,
                                                                        tratar_dados, salvar_dados)
from pipelines.rj_cor.meteorologia.precipitacao_alertario.schedules import minute_schedule
from pipelines.utils.tasks import upload_to_gcs

with Flow('COR: Meteorologia - Pluviometria ALERTARIO') as flow:
    CURRENT_TIME = pendulum.now('America/Sao_Paulo')

    DATASET_ID = 'meio_ambiente_clima'
    TABLE_ID = 'meteorologia_inmet'

    data, hora, minuto = slice_data(current_time=CURRENT_TIME)

    dados = download(data=data)
    dados = tratar_dados(dados=dados, hora=hora)
    path = salvar_dados(dados=dados)
    upload_to_gcs(path=path, dataset_id=DATASET_ID, table_id=TABLE_ID)

# para rodar na cloud
flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
flow.schedule = minute_schedule
