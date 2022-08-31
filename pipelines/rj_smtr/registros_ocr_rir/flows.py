# -*- coding: utf-8 -*-
"""
Flows for monitoramento_rock_in_rio
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

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.registros_ocr_rir.tasks import (
    get_files_from_ftp,
    download_and_save_local,
    pre_treatment_ocr,
)
from pipelines.rj_smtr.tasks import bq_upload

from pipelines.rj_smtr.schedules import every_minute_dev
from pipelines.utils.decorators import Flow

with Flow(
    "SMTR - Captura FTP - CET OCR",
    code_owners=[
        "caio",
        "fernanda",
    ],
) as captura_ocr:
    # SETUP
    dump = Parameter("dump", default=False)
    execution_time = Parameter("execution_time", default=None)
    # Pipeline
    status = get_files_from_ftp(dump=dump, execution_time=execution_time)
    with case(status["capture"], True):
        files = download_and_save_local(
            file_info=status["file_info"],
        )
        updated_status = pre_treatment_ocr(file_info=files)
        with case(updated_status["skip_upload"], False):
            bq_upload(
                dataset_id=constants.RIR_DATASET_ID.value,
                table_id=constants.RIR_TABLE_ID.value,
                filepath=updated_status["table_dir"],
            )

captura_ocr.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_ocr.run_config = KubernetesRun(image=emd_constants.DOCKER_IMAGE.value)
captura_ocr.schedule = every_minute_dev
