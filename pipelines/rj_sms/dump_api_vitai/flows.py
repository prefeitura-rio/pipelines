# -*- coding: utf-8 -*-
from prefect import Parameter
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
    download_api_hist,
)
from pipelines.rj_sms.dump_api_vitai.tasks import conform_csv_to_gcp
from pipelines.rj_sms.dump_api_vitai.scheduler import every_day_at_six_am


with Flow(
    name="SMS: Dump Vitai - Captura Posição de Estoque", code_owners=["thiago"]
) as dump_vitai_estoque:
    # Set Parameters
    #  Vault
    vault_path = "estoque_vitai"
    vault_key = "token"
    #  GCP
    dataset_id = "dump_vitai"
    table_id = "estoque_posicao"
    dump_mode = Parameter("dump_mode", default="append")  # append / overwrite

    # Start run
    download_task = download_api(
        url="https://apidw.vitai.care/api/dw/v1/produtos/saldoAtual",
        destination_file_name=table_id,
        vault_path=vault_path,
        vault_key=vault_key,
        add_load_date_to_filename=True,
    )

    conversion_task = from_json_to_csv(input_path=download_task, sep=";")
    conversion_task.set_upstream(download_task)

    conform_task = conform_csv_to_gcp(input_path=conversion_task, date=None)
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


dump_vitai_estoque.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitai_estoque.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

dump_vitai_estoque.schedule = every_day_at_six_am


with Flow(
    name="SMS: Dump Vitai - Captura Saída de Estoque", code_owners=["thiago"]
) as dump_vitai_saida:
    # Set Parameters
    #  Vault
    vault_path = "estoque_vitai"
    vault_key = "token"
    #  GCP
    dataset_id = "dump_vitai"
    table_id = "estoque_saida"
    dump_mode = Parameter("dump_mode", default="append")  # append / overwrite
    date = Parameter("date", default="2023-09-11")

    # Start run
    download_task = download_api_hist(
        url=f"https://apidw.vitai.care/api/dw/v1/movimentacaoProduto/query/dataMovimentacao",
        destination_file_name=table_id,
        vault_path=vault_path,
        vault_key=vault_key,
        add_load_date_to_filename=True,
        params=date,
    )

    conversion_task = from_json_to_csv(input_path=download_task, sep=";")
    conversion_task.set_upstream(download_task)

    conform_task = conform_csv_to_gcp(input_path=conversion_task, date=date)
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


dump_vitai_saida.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_vitai_saida.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

dump_vitai_saida.schedule = every_day_at_six_am
