# -*- coding: utf-8 -*-
from prefect import Parameter, Flow
from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
)
from pipelines.rj_sms.utils import clean_ascii, download_api
from pipelines.rj_sms.dump_vitai.tasks import fix_payload_vitai


with Flow(
    name="SMS: Dump Vitai - Captura de dados Vitai", code_owners=["thiago"]
) as dump_vitai:
    # Set Parameters
    file_name = Parameter("file_name", default="estoque")
    vault_secret_path = "estoque_vitai"
    vault_secret_key = "token"
    #  GCP
    dataset_id = "dump_vitai"
    table_id = "estoque_posicao"
    dump_mode = "overwrite"  # append or overwrite

    # Start run
    download_task = download_api(
        url="https://apidw.vitai.care/api/dw/v1/produtos/saldoAtual",
        vault_secret_path=vault_secret_path,
        vault_secret_key=vault_secret_key,
        destination_file_name=file_name,
    )

    clean_task = clean_ascii(input_file_path=download_task)
    clean_task.set_upstream(download_task)

    fix_payload_task = fix_payload_vitai(clean_task)
    fix_payload_task.set_upstream(clean_task)

    upload_task = create_table_and_upload_to_gcs(
        data_path=fix_payload_task,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=True,
        wait=None,
    )
    upload_task.set_upstream(fix_payload_task)


dump_vitai.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitai.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)
