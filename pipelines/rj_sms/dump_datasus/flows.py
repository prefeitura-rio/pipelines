
from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from pipelines.utils.dump_url.tasks import download_url, dump_files
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
)

with Flow(
    name= "SMS: Dados Mestres - Captura DataSUS",
    code_owners=[
        "thiago",
    ],
) as dump_datasus:
    
    download_task = download_url(url="ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_202307.ZIP",
                                 url_type="direct")