# -*- coding: utf-8 -*-
from prefect import Parameter, Flow
from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
)
from pipelines.rj_sms.utils import (
    download_api,
    from_json_to_csv,
)
from pipelines.rj_sms.dump_api_vitai.tasks import conform_csv_to_gcp


with Flow(
    name="SMS: Dump Vitai - Captura de dados Vitai", code_owners=["thiago"]
) as dump_vitai:
    # Set Parameters
    #  Vault
    vault_path = "estoque_vitai"
    vault_key = "token"
    #  GCP
    dataset_id = "dump_vitai"
    table_id = "estoque_posicao"
    dump_mode = "append"  # append or overwrite

    # Start run
    download_task = download_api(
        url="https://apidw.vitai.care/api/dw/v1/produtos/saldoAtual",
        destination_file_name=table_id,
        vault_path=vault_path,
        vault_key=vault_key,
    )

    conversion_task = from_json_to_csv(input_path=download_task, sep=";")
    conversion_task.set_upstream(download_task)

    conform_task = conform_csv_to_gcp(input_path=conversion_task)
    conform_task.set_upstream(conversion_task)

    upload_task = create_table_and_upload_to_gcs(
        data_path=conform_task,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=True,
        wait=None,
    )
    upload_task.set_upstream(conform_task)


dump_vitai.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitai.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)
