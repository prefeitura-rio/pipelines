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
    clean_ascii, 
    convert_to_parquet)
from pipelines.rj_sms.dump_vitai.tasks import (
    download_api,
)


with Flow(
    name="SMS: Dump Vitai - Captura de dados Vitai", code_owners=["thiago"]
) as dump_vitai:
    # Set Parameters
    file_name = Parameter("file_name", default="estoque")
    #  GCP
    dataset_id = "dump_vitai"
    table_id = "riosaude_estoque_posicao"
    dump_mode = "append"  # append or overwrite

    # Start run
    download_task = download_api(
        url="https://apidw.vitai.care/api/dw/v1/produtos/saldoAtual",
        file_name = file_name,
    )

    clean_task = clean_ascii(input_file_path = download_task)
    clean_task.set_upstream(download_task)

    to_parquet_task = convert_to_parquet(input_file_path = clean_task,
                                         schema = {"cnes": "str"})
    to_parquet_task.set_upstream(clean_task)

    upload_task = create_table_and_upload_to_gcs(
        data_path=to_parquet_task,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=True,
        wait=None,
    )
    upload_task.set_upstream(to_parquet_task)


dump_vitai.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitai.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)
