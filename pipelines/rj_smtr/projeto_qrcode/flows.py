from prefect import Flow

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.rj_smtr.qrcode.taks import  

from pipelines.rj_smtr.qrcode.taks import (
   open_data,
   xptoB
)


with Flow(
    name="smtr:qrcode - Atualização automática",
    ),
) as flow:
    date_now= datetime.datetime(2022,5,1)
    faixa_horaria={'Manhã': (5 ,11), 'Tarde/Noite':(14,20 ),'Noturno':(22,4)}

    rota_ops=open_data(path_df="frota_ops")
    frota_det=open_data(path_df="frotdet")
    calendar=open_data(path_df="calendar_sppo")
    resultado = xptoB(frota_ops=frota_ops,frota_det=frota_det,calendar,date_now=date_now,faixa_horaria=faixa_horaria)