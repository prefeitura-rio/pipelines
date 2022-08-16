# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_rdo
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

# from copy import deepcopy
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants as emd_constants
from pipelines.rj_smtr.br_rj_riodejaneiro_rdo.tasks import (
    get_file_paths_from_ftp,
    download_and_save_local_from_ftp,
    pre_treatment_br_rj_riodejaneiro_rdo,
)
from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.tasks import bq_upload, get_current_timestamp
from pipelines.rj_smtr.schedules import ftp_schedule

# from pipelines.rj_smtr.br_rj_riodejaneiro_rdo.schedules import every_two_weeks
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import rename_current_flow_run_now_time

with Flow(
    "SMTR - Captura FTPS",
    code_owners=["caio", "fernanda"],
) as captura_ftp:
    transport_mode = Parameter("transport_mode", "SPPO")
    report_type = Parameter("report_type", "RHO")
    table_id = Parameter("table_id", constants.SPPO_RDO_TABLE_ID.value)

    rename_run = rename_current_flow_run_now_time(
        prefix=f"Captura FTPS - {transport_mode}-{report_type} ",
        now_time=get_current_timestamp(),
        wait=None,
    )
    # Parse FTPS
    file_info = get_file_paths_from_ftp(
        transport_mode=transport_mode, report_type=report_type, wait=rename_run
    )
    updated_info = download_and_save_local_from_ftp(file_info=file_info)
    treated_info = pre_treatment_br_rj_riodejaneiro_rdo(file_info=updated_info)
    bq_upload(
        dataset_id=constants.RDO_DATASET_ID.value,
        table_id=table_id,
        filepath=treated_info["treated_path"],
        raw_filepath=treated_info["raw_path"],
        partitions=treated_info["partitions"],
    )


captura_ftp.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_ftp.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
captura_ftp.schedule = ftp_schedule

# captura_sppo_rho = deepcopy(captura_sppo_rdo)
# captura_sppo_rho.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
# captura_sppo_rho.run_config = KubernetesRun(image=emd_constants.DOCKER_IMAGE.value)
