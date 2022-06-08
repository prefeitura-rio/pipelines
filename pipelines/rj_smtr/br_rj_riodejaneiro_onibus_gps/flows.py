# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_onibus_gps
"""

###############################################################################
#
# Aqui é onde devem ser definidos os flows do projeto.
# Cada flow representa uma sequência de passos que serão executados
# em ordem.
#
# Mais informações sobre flows podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/flows.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# Existem diversas maneiras de declarar flows. No entanto, a maneira mais
# conveniente e recomendada pela documentação é usar a API funcional.
# Em essência, isso implica simplesmente na chamada de funções, passando
# os parâmetros necessários para a execução em cada uma delas.
#
# Também, após a definição de um flow, para o adequado funcionamento, é
# mandatório configurar alguns parâmetros dele, os quais são:
# - storage: onde esse flow está armazenado. No caso, o storage é o
#   próprio módulo Python que contém o flow. Sendo assim, deve-se
#   configurar o storage como o pipelines.rj_smtr
# - run_config: para o caso de execução em cluster Kubernetes, que é
#   provavelmente o caso, é necessário configurar o run_config com a
#   imagem Docker que será usada para executar o flow. Assim sendo,
#   basta usar constants.DOCKER_IMAGE.value, que é automaticamente
#   gerado.
# - schedule (opcional): para o caso de execução em intervalos regulares,
#   deve-se utilizar algum dos schedules definidos em schedules.py
#
# Um exemplo de flow, considerando todos os pontos acima, é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
# from prefect import Flow
# from prefect.run_configs import KubernetesRun
# from prefect.storage import GCS
# from pipelines.constants import constants
# from my_tasks import my_task, another_task
# from my_schedules import some_schedule
#
# with Flow("my_flow") as flow:
#     a = my_task(param1=1, param2=2)
#     b = another_task(a, param3=3)
#
# flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow.schedule = some_schedule
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################


from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants as emd_constants
from pipelines.rj_smtr.tasks import (
    create_current_date_hour_partition,
    create_local_partition_path,
    fetch_dataset_sha,
    get_date_range,
    # get_local_dbt_client,
    get_raw,
    run_dbt_command,
    save_raw_local,
    save_treated_local,
    set_last_run_timestamp,
    upload_logs_to_bq,
    bq_upload,
)
from pipelines.rj_smtr.br_rj_riodejaneiro_onibus_gps.constants import constants
from pipelines.rj_smtr.br_rj_riodejaneiro_onibus_gps.schedules import (
    every_minute,
    every_hour,
)
from pipelines.rj_smtr.br_rj_riodejaneiro_onibus_gps.tasks import (
    pre_treatment_br_rj_riodejaneiro_onibus_gps,
)

from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client

with Flow(
    "SMTR - Materializar - br_rj_riodejaneiro_veiculos.gps_sppo",
    code_owners=["@hellcassius#1223", "@fernandascovino#9750"],
) as materialize:
    dataset_id = Parameter("dataset_id", "br_rj_riodejaneiro_onibus_gps")
    table_id = Parameter("table_id", "sppo_aux_registros_filtrada")
    rebuild = Parameter("rebuild", False)
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)
    dbt_client = get_k8s_dbt_client(mode="prod")
    date_range = get_date_range(dataset_id, table_id)
    dataset_sha = fetch_dataset_sha(
        dataset_id=dataset_id,
    )
    with case(rebuild, True):
        RUN = run_dbt_command(
            dbt_client=dbt_client,
            dataset_id=dataset_id,
            table_id=table_id,
            command="run",
            _vars=[date_range, dataset_sha],
            upstream=True,
            downstream=True,
            flags="-- full-refresh",
        )
    with case(rebuild, False):
        RUN = run_dbt_command(
            dbt_client=dbt_client,
            dataset_id=dataset_id,
            table_id=table_id,
            command="run",
            _vars=[date_range, dataset_sha],
            upstream=True,
            downstream=True,
        )
    set_last_run_timestamp(dataset_id=dataset_id, table_id=table_id, wait=RUN)


with Flow(
    "SMTR - Captura - GPS SPPO",
    code_owners=["@hellcassius#1223", "@fernandascovino#9750"],
) as captura_sppo:

    kind = Parameter("kind", default="sppo")
    dataset_id = Parameter("dataset_id", default=constants.SPPO_DATASET_ID.value)
    secret_path = Parameter("sppo_secret", default=constants.SPPO_SECRET_PATH.value)
    table_id = Parameter("table_id", default=constants.SPPO_TABLE_ID.value)
    url = Parameter("url", default=constants.BASE_URL.value)

    file_dict = create_current_date_hour_partition()

    filepath = create_local_partition_path(
        dataset_id=dataset_id,
        table_id=table_id,
        filename=file_dict["filename"],
        partitions=file_dict["partitions"],
    )

    status_dict = get_raw(url=url, kind="sppo")

    raw_filepath = save_raw_local(data=status_dict["data"], file_path=filepath)

    treated_status = pre_treatment_br_rj_riodejaneiro_onibus_gps(
        status_dict=status_dict
    )

    upload_logs_to_bq(
        dataset_id=dataset_id,
        parent_table_id=table_id,
        timestamp=status_dict["timestamp"],
        error=status_dict["error"],
    )

    treated_filepath = save_treated_local(
        dataframe=treated_status["df"], file_path=filepath
    )

    bq_upload(
        dataset_id=dataset_id,
        table_id=table_id,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=file_dict["partitions"],
    )

materialize.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
materialize.run_config = KubernetesRun(image=emd_constants.DOCKER_IMAGE.value)
materialize.schedule = every_hour
captura_sppo.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_sppo.run_config = KubernetesRun(image=emd_constants.DOCKER_IMAGE.value)
captura_sppo.schedule = every_minute
