"""
Flows for emd
"""
from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.rj_cor.alagamentos.satelite.tasks import (slice_data, download,
                                                      tratar_dados, salvar_parquet,
                                                      upload_to_gcs)
from pipelines.rj_cor.alagamentos.satelite.schedules import hour_schedule

with Flow('goes_16') as flow:
    CURRENT_TIME = Parameter('CURRENT_TIME', default=None)  #or pendulum.now("utc")
    VARIAVEL = Parameter('VARIAVEL', default=None)
    DATASET_ID = Parameter('DATASET_ID', default=None)
    TABLE_ID = Parameter('TABLE_ID', default=None)

    if CURRENT_TIME is None:
        print('Problema no current_time')
    elif VARIAVEL is None:
        print('Problema na variável')

    ano, mes, dia, hora, dia_juliano = slice_data(current_time=CURRENT_TIME)

    filename = download(variavel=VARIAVEL,
                        ano=ano,
                        dia_juliano=dia_juliano,
                        hora=hora)
    info = tratar_dados(filename=filename)
    path = salvar_parquet(info=info)
    upload_to_gcs(path=path, dataset_id=DATASET_ID, table_id=TABLE_ID)

# para rodar na cloud
flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
flow.schedule = hour_schedule
