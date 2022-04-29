from prefect import Flow

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_smtr.qrcode.taks import  

from pipelines.rj_smtr.qrcode.taks import (
   get_service_Sigmod,
   SV_SN_Filter
)


with Flow(
    name="smtr:qrcode - Atualização automática",
    ),
) as flow:

            #pegar tabela       br_rj_sigmob
            #                           - frota_determinada 
            sigmob_data = get_service_Sigmod()
            service_data = filter_by_tag(sigmob_data)

            #pegar tabela       br_rj_riodejaneiro_onibus_gps
            #                           - aux_registros_filtrada
            #filtrar linhas < 20%
