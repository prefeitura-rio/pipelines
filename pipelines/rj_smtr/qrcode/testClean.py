"""
past 
Puxa informações do sigmob -> https://rj-smtr.github.io/sigmob-api-docs/
Remove serviços com nome contendo SV e SN


ing
Calcula linhas operantes -> cruza com dados de gps (leitura de dados do datalake)
    Linha com operação abaixo de 20% do determinado. (Pegar a maior hora cheia do dia para fins desse cálculo) -> tabela frota_determinada do sigmob
    Últimos 5 dias úteis com a mesma situação

future
Atualiza as fixtures (json) no Django
(extra) Adicionar período de funcionamento
[mob-app] Adicionar período de funcionamento RJ-SMTR/mobilidade-rio-api#42
"""




##### puxa sigmob ######

import requests
import regex as re

url ="https://jeap.rio.rj.gov.br/MOB/get_routes.rule?sys=MOB"
r = requests.get(url)
#data = SV_SN_Filter(r.json)

########################



##### remove SN|SV ######

services_list=[]

for item in r.json()["result"]:#data: 
    
    if not re.search(r'SV|SN\b', item["SiglaServico"]):
    
        services_list.append(item)
        
########################

##### puxa datalake ######
#pegar tabela       br_rj_sigmob
#                           - frota_determinada 
#pegar tabela       br_rj_riodejaneiro_onibus_gps
#                           - aux_registros_filtrada
#filtrar linhas < 20%


'''
@task(checkpoint=False)
def get_data() -> pd.DataFrame:
    """
    Returns the dataframe with the alerts.
    """
    query = """
    SELECT *
    FROM `rj-escritorio-dev.transporte_rodoviario_waze.alertas_tratadas_semaforos`
    """
    # TODO change billing_project_id to rj-cor
    return bd.read_sql(query=query, billing_project_id="datario", from_file=True)
    '''
    
########################