import datetime

import basedosdados as bd
import pandas as pd
import numpy as np
#
import requests
import yaml

##### puxa datalake ######
def get_data(query) -> pd.DataFrame:
    """
    Returns the dataframe from basedosdados.
    """
    
    return bd.read_sql(query=query, billing_project_id="rj-smtr-dev", from_file=True)

def get_frota_ops() -> pd.DataFrame:
    """
    Returns the dataframe with sigmob bus fleet.
    """

    #stage 1.0 - pegar frota determinada
    query = """  
            select 
                data,  
                servico,
                extract(hour from hora) hora,
                count(distinct id_veiculo) frota_aferida
            from `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
            where
    """

    ### Parametrização ###
    interval= get_date_range(10)#setar como parametro do pipeline
    query+=" DATE(data) between date('{} 00:00:00') and date('{} 23:00:00') and datetime(concat(cast(data as string), ' ', hora)) > '{} 00:00:00' and datetime(concat(cast(data as string), ' ', hora)) <= '{} 23:00:00'".format(interval[0],interval[1],interval[0],interval[1])
    query += """
                and flag_em_operacao = true 
                and servico is not null 
                group by data , servico, hora 
                order by data desc, hora asc,frota_aferida desc
    """
    ######################
    
    frota_ops=get_data(query)
    frota_ops = frota_ops[~frota_ops["servico"].str.contains('SV|SN|SP|SR|SE|LECD')]#testar
    return frota_ops
    

def get_frota_determinada() -> pd.DataFrame:
    """
    Returns the dataframe with sigmob bus fleet.
    """

    #stage 1.0 - pegar frota determinada
    query = """  
                select 
                    data,
                    data_versao,
                    route_short_name,
                    frota_servico,
                    data_feriado
                from(
                    select 
                        f.data,
                        data_feriado,
                        DATE(f.data_versao) data_versao,
                        route_short_name,
                        sum(FrotaServico) frota_servico,
                        -- consorcio
                    from (
                        select * 
                        from rj-smtr.br_rj_riodejaneiro_sigmob.frota_determinada_desaninhada f1
                        join (
                            select
                                data,
                                -- data_versao_efetiva_agency,
                                data_versao_efetiva_frota_determinada,
                                data_versao_efetiva_holidays,
                                data_versao_efetiva_routes,
                            from rj-smtr.br_rj_riodejaneiro_sigmob.data_versao_efetiva) d
                    on 
                        DATE(f1.data_versao) = d.data_versao_efetiva_frota_determinada) f
                    join (
                        select  
                            route_id, 
                            -- agency_id,
                            route_short_name,
                            data_versao
                        from rj-smtr.br_rj_riodejaneiro_sigmob.routes_desaninhada
                    ) r
                    on 
                        f.route_id = r.route_id
                    and 
                        DATE(f.data_versao_efetiva_routes) = r.data_versao

                    left join (
                        select 
                            data as data_feriado,
                            data_versao
                        from rj-smtr.br_rj_riodejaneiro_sigmob.holidays
                        ) h
                    on
                        f.data_versao_efetiva_holidays = DATE(h.data_versao)
                        and f.data = h.data_feriado
                    group by 
                        f.data, data_feriado, f.data_versao, route_short_name
                    order by f.data desc
                )
                where
    """
    #DATE(data) between date('2022-03-02 10:10:10') and date('2022-03-05 10:10:10')
    ### Parametrização ###
    interval= get_date_range(10)#setar como parametro do pipeline
    query += " DATE(data) between date('{} 00:00:00') and date('{} 00:00:00')".format(interval[0],interval[1])
    ######################
    
    sigmob_frota=get_data(query)
    sigmob_frota = sigmob_frota[~sigmob_frota["route_short_name"].str.contains('SV|SN|SP|SR|SE|LECD')]
    sigmob_frota = sigmob_frota.replace({'data_feriado': pd.NaT}, {'data_feriado': False})
    return sigmob_frota


def get_calendar_sspo() -> pd.DataFrame:
    """
    Returns the dataframe with service schedule.
    """
    
    #stage 1.1 - calendario de operacao 
    query = """  
            select 
                route_id, 
                route_short_name,
                monday,
                tuesday,
                wednesday,
                thursday,
                friday,
                saturday,
                sunday,
                holiday
            from `rj-smtr-dev.projeto_qrcode.sppo_calendar`
            where
    """

    query += " " +datetime.datetime.today().strftime('%A').lower() +" = '1'"
    sppo_calendar_today = get_data(query)
    # ~ marca um operador NOT
    sppo_calendar_today = sppo_calendar_today[~sppo_calendar_today["route_short_name"].str.contains('SV|SN|SP|SR|SE|LECD')]
    return sppo_calendar_today


def get_date_range(date_interval) -> (str,str):#setar date_interval como parametro do pipeline
    """
    Returns tuple of date strings (10 days before today, today)
    """
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=date_interval)#pega 10 dias pq não sabemos os feriados.
    return (start_date.strftime('%Y-%m-%d'),end_date.strftime('%Y-%m-%d'))

def xptoA(frota_ops,frota_det,calendar,date_now)->pd.DataFrame:
    """
    Returns dataframe regarding operation of the last 5 days.
    """
    datas =  frota_det['data'].unique() #formatar data
    linhas = frota_det['route_short_name'].unique()
    frota_operante=pd.DataFrame([],index=linhas,columns=datas)
    frota_operante['operante']=True
    
    for item in linhas:
        crl=0

        for i in range(0,10):#setar range = date_interval como parametro do pipeline # pegar do date.unique
            date = date_now - datetime.timedelta(days=i)
            
            #pegar por servico e data e hora e pegar a maior  para saber frota ops        
            frota_ops_dia = frota_ops.loc[frota_ops['servico'] == item]
            frota_ops_dia = frota_ops_dia.loc[frota_ops_dia['data']==date.strftime('%Y-%m-%d')]
            #print(frota_ops_dia['data'].unique(),frota_ops_dia['frota_aferida'].max())

            #pegar a frota det por dia
            frota=frota_det.loc[frota_det['route_short_name'] == item]
            frota=frota[frota['data']==date.strftime('%Y-%m-%d')]

            #cruza para pegar feriado e dia de operação
            roda_feriado = calendar[calendar['route_short_name']==item]['holiday'].squeeze() =="1" and frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].squeeze()
                    
            if calendar[calendar['route_short_name']==item][date.strftime('%A').lower()].squeeze()=="1" or  roda_feriado:
                crl+=1
                percent=frota_ops_dia['frota_aferida'].max()/frota[frota['data']==date.strftime('%Y-%m-%d')]['frota_servico'].squeeze()
                if type(percent) == np.float64:
                    frota_operante[date].loc[item]=percent
                else:
                    frota_operante[date].loc[item]=0
            
            if crl <=5 and frota_operante[date].loc[item] == frota_operante[date].loc[item]: # frota_operante[date].loc[item] == frota_operante[date].loc[item] checks not NaN
                frota_operante['operante'].loc[item]=frota_operante[date].loc[item] >= 0.2 and frota_operante['operante'].loc[item]
    
    return frota_operante

def xptoB(frota_ops,frota_det,calendar,date_now,faixa_horaria)->pd.DataFrame:
    """
    Returns dataframe regarding operation of the last 5 days.
    """

    datas = frota_det['data'].unique() #formatar data
    linhas = frota_det['route_short_name'].unique()
    frota_operante=pd.DataFrame([],index=linhas,columns=datas)
    frota_operante['operante']=True
    frota_operante['operante_faixa']="Sem previsão de funcionamento"      
    #print(frota_operante)


    #################### manha ##########################
    frota_ops_manha=frota_ops.loc[frota_ops['hora'] >= faixa_horaria['manhã'][0]]  
    frota_ops_manha=frota_ops_manha.loc[frota_ops['hora'] < faixa_horaria['manhã'][1]] 
   
    for item in linhas:
        crl=0

        for i in range(0,10):#setar range = date_interval como parametro do pipeline # pegar do date.unique
            date = date_now - datetime.timedelta(days=i)
            
            #pegar por servico e data e faixa e pegar a maior  para saber frota ops        
            frota_ops_manha_dia = frota_ops_manha.loc[frota_ops['servico'] == item]
            frota_ops_manha_dia = frota_ops_manha_dia.loc[frota_ops_manha_dia['data']==date.strftime('%Y-%m-%d')]
            #print(frota_ops_dia['data'].unique(),frota_ops_dia['frota_aferida'].max())

            #pegar a frota det por dia
            frota=frota_det.loc[frota_det['route_short_name'] == item]
            frota=frota[frota['data']==date.strftime('%Y-%m-%d')]

            #cruza para pegar feriado e dia de operação
            roda_feriado = calendar[calendar['route_short_name']==item]['holiday'].squeeze() =="1" and frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].squeeze()
                    
            if calendar[calendar['route_short_name']==item][date.strftime('%A').lower()].squeeze()=="1" or  roda_feriado:
                crl+=1
                percent=frota_ops_manha_dia['frota_aferida'].max()/frota[frota['data']==date.strftime('%Y-%m-%d')]['frota_servico'].squeeze()
                if type(percent) == np.float64:
                    frota_operante[date].loc[item]=percent
                else:
                    frota_operante[date].loc[item]=0
            
            if crl <=5 and frota_operante[date].loc[item] == frota_operante[date].loc[item]: # frota_operante[date].loc[item] == frota_operante[date].loc[item] checks not NaN

                #verifica domingo  and feriado 
                if date.strftime('%A').lower()=="sunday" and frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].squeeze():
                    frota_operante['operante'].loc[item]=frota_operante[date].loc[item] >= 0.4 and frota_operante['operante'].loc[item]
                else:
                    frota_operante['operante'].loc[item]=frota_operante[date].loc[item] >= 0.2 and frota_operante['operante'].loc[item]
        
        if frota_operante['operante'].loc[item] and frota_operante['operante_faixa'].loc[item]=="Sem previsão de funcionamento":
            frota_operante['operante_faixa'].loc[item]="Manhã"
        elif frota_operante['operante_faixa'].loc[item]!="Sem previsão de funcionamento":
            frota_operante['operante_faixa'].loc[item]+=" Manhã"
    
    ######################################################


    ######################Tarde/nooite ######################
    frota_ops_tarde_noite=frota_ops.loc[frota_ops['hora'] >= faixa_horaria['Tarde/Noite'][0]]  
    frota_ops_tarde_noite=frota_ops_tarde_noite.loc[frota_ops['hora'] < faixa_horaria['Tarde/Noite'][1]]
    for item in linhas:
        crl=0

        for i in range(0,10):#setar range = date_interval como parametro do pipeline # pegar do date.unique
            date = date_now - datetime.timedelta(days=i)
            
            #pegar por servico e data e faixa e pegar a maior  para saber frota ops        
            frota_ops_tarde_noite_dia = frota_ops_tarde_noite.loc[frota_ops['servico'] == item]
            frota_ops_tarde_noite_dia = frota_ops_tarde_noite_dia.loc[frota_ops_tarde_noite_dia['data']==date.strftime('%Y-%m-%d')]
            #print(frota_ops_dia['data'].unique(),frota_ops_dia['frota_aferida'].max())

            #pegar a frota det por dia
            frota=frota_det.loc[frota_det['route_short_name'] == item]
            frota=frota[frota['data']==date.strftime('%Y-%m-%d')]

            #cruza para pegar feriado e dia de operação
            roda_feriado = calendar[calendar['route_short_name']==item]['holiday'].squeeze() =="1" and frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].squeeze()
                    
            if calendar[calendar['route_short_name']==item][date.strftime('%A').lower()].squeeze()=="1" or  roda_feriado:
                crl+=1
                percent=frota_ops_tarde_noite_dia['frota_aferida'].max()/frota[frota['data']==date.strftime('%Y-%m-%d')]['frota_servico'].squeeze()
                if type(percent) == np.float64:
                    frota_operante[date].loc[item]=percent
                else:
                    frota_operante[date].loc[item]=0
            
            if crl <=5 and frota_operante[date].loc[item] == frota_operante[date].loc[item]: # frota_operante[date].loc[item] == frota_operante[date].loc[item] checks not NaN
                #verifica domingo  and feriado 
                if date.strftime('%A').lower()=="sunday" and frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].squeeze():
                    frota_operante['operante'].loc[item]=frota_operante[date].loc[item] >= 0.4 and frota_operante['operante'].loc[item]
                else:
                    frota_operante['operante'].loc[item]=frota_operante[date].loc[item] >= 0.2 and frota_operante['operante'].loc[item]
        
        if frota_operante['operante'].loc[item] and frota_operante['operante_faixa'].loc[item]=="Sem previsão de funcionamento":
            frota_operante['operante_faixa'].loc[item]="Tarde/Noite"
        elif frota_operante['operante_faixa'].loc[item]!="Sem previsão de funcionamento":
            frota_operante['operante_faixa'].loc[item]+=" Tarde/Noite"
    
    ######################################################

    
    #######################Noturno######################
    frota_ops_noturno=frota_ops.loc[frota_ops['hora'] >= faixa_horaria['Noturno'][0]]  
    frota_ops_noturno=frota_ops_noturno.loc[frota_ops['hora'] < faixa_horaria['Noturno'][1]] 
    
    for item in linhas:
        crl=0

        for i in range(0,10):#setar range = date_interval como parametro do pipeline # pegar do date.unique
            date = date_now - datetime.timedelta(days=i)
            
            #pegar por servico e data e faixa e pegar a maior  para saber frota ops        
            frota_ops_noturno_dia = frota_ops_noturno.loc[frota_ops['servico'] == item]
            frota_ops_noturno_dia = frota_ops_noturno_dia.loc[frota_ops_noturno_dia['data']==date.strftime('%Y-%m-%d')]
            #print(frota_ops_dia['data'].unique(),frota_ops_dia['frota_aferida'].max())

            #pegar a frota det por dia
            frota=frota_det.loc[frota_det['route_short_name'] == item]
            frota=frota[frota['data']==date.strftime('%Y-%m-%d')]

            #cruza para pegar feriado e dia de operação
            roda_feriado = calendar[calendar['route_short_name']==item]['holiday'].squeeze() =="1" and frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].squeeze()
                    
            if calendar[calendar['route_short_name']==item][date.strftime('%A').lower()].squeeze()=="1" or  roda_feriado:
                crl+=1
                percent=frota_ops_noturno_dia['frota_aferida'].max()/frota[frota['data']==date.strftime('%Y-%m-%d')]['frota_servico'].squeeze()
                if type(percent) == np.float64:
                    frota_operante[date].loc[item]=percent
                else:
                    frota_operante[date].loc[item]=0
            
            if crl <=5 and frota_operante[date].loc[item] == frota_operante[date].loc[item]: # frota_operante[date].loc[item] == frota_operante[date].loc[item] checks not NaN
                #verifica domingo  and feriado 
                if date.strftime('%A').lower()=="sunday" and frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].squeeze():
                    frota_operante['operante'].loc[item]=frota_operante[date].loc[item] >= 0.4 and frota_operante['operante'].loc[item]
                else:
                    frota_operante['operante'].loc[item]=frota_operante[date].loc[item] >= 0.2 and frota_operante['operante'].loc[item]
        
        if frota_operante['operante'].loc[item] and frota_operante['operante_faixa'].loc[item]=="Sem previsão de funcionamento":
            frota_operante['operante_faixa'].loc[item]="Noturno"
        elif frota_operante['operante_faixa'].loc[item]!="Sem previsão de funcionamento":
            frota_operante['operante_faixa'].loc[item]+=" Noturno"
    
    ######################################################

    
    ######################hora util########################
    frota_ops_hora_util=frota_ops.loc[frota_ops['hora'] >= faixa_horaria['Hora útil'][0]]  
    frota_ops_hora_util=frota_ops_hora_util.loc[frota_ops['hora'] < faixa_horaria['Hora útil'][1]]

    for item in linhas:
        crl=0

        for i in range(0,10):#setar range = date_interval como parametro do pipeline # pegar do date.unique
            date = date_now - datetime.timedelta(days=i)
            
            #pegar por servico e data e faixa e pegar a maior  para saber frota ops        
            frota_ops_hora_util_dia = frota_ops_hora_util.loc[frota_ops['servico'] == item]
            frota_ops_hora_util_dia = frota_ops_hora_util_dia.loc[frota_ops_hora_util_dia['data']==date.strftime('%Y-%m-%d')]
            #print(frota_ops_dia['data'].unique(),frota_ops_dia['frota_aferida'].max())

            #pegar a frota det por dia
            frota=frota_det.loc[frota_det['route_short_name'] == item]
            frota=frota[frota['data']==date.strftime('%Y-%m-%d')]

            #cruza para pegar feriado e dia de operação
            roda_feriado = calendar[calendar['route_short_name']==item]['holiday'].squeeze() =="1" and frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].squeeze()
                    
            if calendar[calendar['route_short_name']==item][date.strftime('%A').lower()].squeeze()=="1" or  roda_feriado:
                crl+=1
                percent=frota_ops_hora_util_dia['frota_aferida'].max()/frota[frota['data']==date.strftime('%Y-%m-%d')]['frota_servico'].squeeze()
                if type(percent) == np.float64:
                    frota_operante[date].loc[item]=percent
                else:
                    frota_operante[date].loc[item]=0
            
            if crl <=5 and frota_operante[date].loc[item] == frota_operante[date].loc[item]: # frota_operante[date].loc[item] == frota_operante[date].loc[item] checks not NaN
                #verifica domingo  and feriado 
                if date.strftime('%A').lower()=="sunday" and frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].squeeze():
                    frota_operante['operante'].loc[item]=frota_operante[date].loc[item] >= 0.4 and frota_operante['operante'].loc[item]
                else:
                    frota_operante['operante'].loc[item]=frota_operante[date].loc[item] >= 0.2 and frota_operante['operante'].loc[item]
        
        if frota_operante['operante'].loc[item] and frota_operante['operante_faixa'].loc[item]=="Sem previsão de funcionamento":
            frota_operante['operante_faixa'].loc[item]="Hora útil"
        elif frota_operante['operante_faixa'].loc[item]!="Sem previsão de funcionamento":
            frota_operante['operante_faixa'].loc[item]+=" Hora útil"
    
    ######################################################
    
    return frota_operante
   

def fetch_sigmob_api(url: str): #-> List[dict]:
    """
    Fetches SIGMOB endpoints, whether they have pagination or not.
    """
    results: list = []
    next: str = url
    while next:
        try:
            print("Fetching %s" % next)
            data = requests.get(next, timeout=DEFAULT_TIMEOUT)
            data.raise_for_status()
            data = data.json()
            if "result" in data:
                results.extend(data["result"])
            elif "data" in data:
                results.extend(data["data"])
            if "next" in data and data["next"] != "EOF" and data["next"] != "":
                next = data["next"]
            else:
                next = None
        except Exception as e:
            raise e
    return results


def parse_data_to_fixture(data: dict, config: dict, i: int) -> dict:
    _fix_null = lambda x: x if x else ""

    result = {
        "model": "pontos."+config["model"],
        "pk": data[config["pk"]] if config["pk"] else i,
        "fields": dict()
        # "fields": {k: _fix_null(data[v]) for k,v in config["fields"].items()}
    }
    # TODO: fix bug on max_lenght field
    for k,v in config["fields"].items():
        if "fix_lenght" in config:
            if k in config["fix_lenght"]:
                data[v] = data[v][:config["fix_lenght"][k]]
        result["fields"][k] = _fix_null(data[v])
    return result

#controle de cosulta para dev
import pickle

def open_data(path_df):

    with open("/home/d/code/SMTR/pipelines/pipelines/rj_smtr/qrcode/"+path_df+".picle", 'rb') as data:
        df = pickle.load(data)
        return df

def save_data(df,df_name):

    with open('/home/d/code/SMTR/pipelines/pipelines/rj_smtr/qrcode/'+df_name+".picle", 'wb') as output:
        pickle.dump(df, output)


#starts here
date_now= datetime.datetime(2022,3,29)#datetime.datetime.now()
frota_ops=open_data("frota_ops")#get_frota_ops()
frota_det=open_data("frotdet")# get_frota_determinada() 
calendar=open_data("calendar_sppo")#get_calendar_sspo()

frota_ops = frota_ops[~frota_ops["servico"].str.contains('SV|SN|SP|SR|SE|LECD')]
#.

#faixa=date_now
#faixa_horaria={'manhã': ( faixa.replace(hour=5) ,faixa.replace(hour=11) ), 'Tarde/Noite':( faixa.replace(hour=14),faixa.replace(hour=20) ),'Hora útil':(faixa.replace(hour=5),faixa.replace(hour=20)),'Noturno':(faixa.replace(hour=22),faixa.replace(hour=4))}

faixa_horaria={'manhã': (5 ,11), 'Tarde/Noite':(14,20 ),'Hora útil':(5,20),'Noturno':(22,4)}

resultado = xptoB(frota_ops,frota_det,calendar,date_now,faixa_horaria)
print(resultado['operante_faixa'])

DEFAULT_TIMEOUT = 60
CONFIG_FILENAME = "config.yaml"
ACTIVE_FILENAME = "trip_id_regular.json"
STOP_FIXTURE_FILENAME = "fixtures/stop.json"
OUTPUT_JSON = f"{{model}}.json"




config = yaml.load(
    open(CONFIG_FILENAME, "r"), 
    Loader=yaml.FullLoader
)["models"]["trip"]#["route"]#marretado aqui
result=fetch_sigmob_api(config["source"])

fixture = []
for i, record in enumerate(result):
    
    record = parse_data_to_fixture(record, config["json"], i)
    #record['fields']=record['fields'].update({'operation_time':resultado.loc[record['fields']['route']]['operante_faixa']}) 
    print (record,type(record))
    exit()
    #if _is_active(record, model):
    #    fixture.append(record)

print (record,type(record))
exit()

