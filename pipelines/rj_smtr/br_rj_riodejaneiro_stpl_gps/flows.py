# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_stpl_gps
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


from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.rj_smtr.tasks import (
    create_current_date_hour_partition,
    create_local_partition_path,
    get_raw,
    save_raw_local,
    save_treated_local,
    upload_logs_to_bq,
    bq_upload,
)
from pipelines.rj_smtr.br_rj_riodejaneiro_stpl_gps.tasks import (
    pre_treatment_br_rj_riodejaneiro_stpl_gps,
)

# from pipelines.rj_smtr.br_rj_riodejaneiro_stpl_gps.schedules import every_minute
from pipelines.utils.decorators import Flow

with Flow(
    "SMTR: gps_stpl - Captura",
    code_owners=["@hellcassius#1223", "@fernandascovino#9750"],
) as stpl_captura:

    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    url = Parameter("url")
    key_column = Parameter("key_column")
    kind = Parameter("kind")

    file_dict = create_current_date_hour_partition()

    filepath = create_local_partition_path(
        dataset_id=dataset_id,
        table_id=table_id,
        filename=file_dict["filename"],
        partitions=file_dict["partitions"],
    )

    status_dict = get_raw(url=url, kind=kind)

    raw_filepath = save_raw_local(data=status_dict["data"], file_path=filepath)

    treated_status = pre_treatment_br_rj_riodejaneiro_stpl_gps(
        status_dict=status_dict, key_column=key_column
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


stpl_captura.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
stpl_captura.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# stpl_captura.schedule = every_minute
