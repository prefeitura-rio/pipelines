"""
Flows for precipitacao_alertario
"""
from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.precipitacao_alertario.tasks import (download,
                                                                        tratar_dados, salvar_dados)
from pipelines.rj_cor.meteorologia.precipitacao_alertario.schedules import minute_schedule
from pipelines.utils.tasks import upload_to_gcs

with Flow('COR: Meteorologia - Pluviometria ALERTARIO') as flow:

    DATASET_ID = 'meio_ambiente_clima'
    TABLE_ID = 'precipitacao_alertario'

    filename, current_time = download()
    dados = tratar_dados(filename=filename)
    path, partitions = salvar_dados(dados=dados, current_time=current_time)
    upload_to_gcs(path=path, dataset_id=DATASET_ID, table_id=TABLE_ID, partitions=partitions)

# para rodar na cloud
flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
flow.schedule = minute_schedule
