
from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from pipelines.rj_sms.dump_datasus.tasks import download_from_ftp
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
)

# TODO: add agent label
with Flow(
    name= "SMS: Dados Mestres - Captura DataSUS",
    code_owners=[
        "thiago",
    ],
) as dump_datasus:

    download_task = download_from_ftp(ftp_url="ftp://ftp.datasus.gov.br/cnes/",
                                      remote_filepath="BASE_DE_DADOS_CNES_202307.ZIP",
                                      local_filepath= "~/temp/BASE_DE_DADOS_CNES_202307.ZIP")
    
# TODO: add GCS & Kubernetes configs