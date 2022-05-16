import datetime

import basedosdados as bd
import pandas as pd
import numpy as np
#
import requests
import yaml


import json
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
                    route_id,
                    data_feriado
                from(
                    select 
                        f.data,
                        f.route_id,
                        data_feriado,
                        DATE(f.data_versao) data_versao,
                        route_short_name,
                        sum(FrotaServico) frota_servico,
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
                        f.data, data_feriado, f.data_versao, route_short_name, f.route_id
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
                    f.route_id,
                    f.old_route_id,
                    c.route_short_name,
                    id_modal,
                    monday,
                    tuesday,
                    wednesday,
                    thursday,
                    friday,
                    saturday,
                    sunday,
                    holiday
                from `rj-smtr-dev.projeto_qrcode_test.calendar` c

            cross join (
                select 
                        route_id,
                        old_route_id,
                        route_short_name,
                        data_versao

                from rj-smtr-dev.br_rj_riodejaneiro_sigmob.routes_desaninhada f1
                join (
                    select
                        data,
                        data_versao_efetiva_routes,
                    from rj-smtr.br_rj_riodejaneiro_sigmob.data_versao_efetiva) d
            on 
            """
    #query += "DATE(f1.data_versao) = d.data_versao_efetiva_routes and d.data = date('{}')) f where f.route_short_name=c.route_short_name and c.{} ='1' and f.route_id is not null".format(datetime.datetime.today().strftime('%Y-%m-%d'),datetime.datetime.today().strftime('%A').lower())
    query += "DATE(f1.data_versao) = d.data_versao_efetiva_routes and d.data = date('{}')) f where f.route_short_name=c.route_short_name and c.{} ='1' and f.route_id is not null".format(date_now.strftime('%Y-%m-%d'),date_now.strftime('%A').lower())

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
    frota_operante['route_short_name']=linhas
    frota_operante['operante_faixa']="Sem previsão de funcionamento"      
    #print(frota_operante)

    #################### manha ##########################
    frota_ops_manha=frota_ops.loc[frota_ops['hora'] >= faixa_horaria['Manhã'][0]]  
    frota_ops_manha=frota_ops_manha.loc[frota_ops['hora'] < faixa_horaria['Manhã'][1]] 
    frota_operante['operante']=True

    for item in linhas:
        crl=0

        for i in range(1,10):#setar range = date_interval como parametro do pipeline # pegar do date.unique
            date = date_now - datetime.timedelta(days=i)
            
            #pegar a frota det por dia
            frota=frota_det.loc[frota_det['route_short_name'] == item]
            frota=frota[frota['data']==date.strftime('%Y-%m-%d')]

            #pegar por servico e data e faixa e pegar a maior  para saber frota ops        
            frota_ops_manha_dia = frota_ops_manha.loc[frota_ops['servico'] == item]
            frota_ops_manha_dia = frota_ops_manha_dia.loc[frota_ops_manha_dia['data']==date.strftime('%Y-%m-%d')]

            #cruza para pegar feriado e dia de operação e decidir se operante ou não
            if not calendar[calendar['route_short_name']==item].empty:
                
                roda_feriado = calendar[calendar['route_short_name']==item]['holiday'].item() =='1' and frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].squeeze()

                if calendar[calendar['route_short_name']==item][date.strftime('%A').lower()].item()=="1" or  roda_feriado:
                    crl+=1
                    
                    if crl <=5: # frota_operante[date].loc[item] == frota_operante[date].loc[item] checks not NaN
                        percent=frota_ops_manha_dia['frota_aferida'].max()/frota[frota['data']==date.strftime('%Y-%m-%d')]['frota_servico'].squeeze()
                        
                        if percent==percent and type(percent) == np.float64:
                            frota_operante[date].loc[item]=percent
                            
                        else:
                            frota_operante[date].loc[item]=0
                            
                        #verifica domingo  and feriado 
                        #no oficio, 80% da frota det dia util | 40% em dom e feriado --> 0.2|0.1
                        if date.strftime('%A').lower()=="sunday" and frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].squeeze():
                            frota_operante['operante'].loc[item]=frota_operante[date].loc[item] >= 0.1 and frota_operante['operante'].loc[item]
                        else:
                            frota_operante['operante'].loc[item]=frota_operante[date].loc[item] >= 0.2 and frota_operante['operante'].loc[item]
                    
            elif crl<5:
                frota_operante['operante'].loc[item] =False

        if frota_operante['operante'].loc[item] and frota_operante['operante_faixa'].loc[item]=="Sem previsão de funcionamento":
            frota_operante['operante_faixa'].loc[item]="Manhã"
        elif frota_operante['operante'].loc[item]:#frota_operante['operante_faixa'].loc[item]!="Sem previsão de funcionamento":
            frota_operante['operante_faixa'].loc[item]+=" Manhã"
    
    ######################################################



    ######################Tarde/nooite ######################
    frota_ops_tarde_noite=frota_ops.loc[frota_ops['hora'] >= faixa_horaria['Tarde/Noite'][0]]  
    frota_ops_tarde_noite=frota_ops_tarde_noite.loc[frota_ops['hora'] < faixa_horaria['Tarde/Noite'][1]]
    frota_operante['operante']=True

    for item in linhas:
        crl=0

        for i in range(1,10):#setar range = date_interval como parametro do pipeline # pegar do date.unique
            date = date_now - datetime.timedelta(days=i)
            
            #pegar a frota det por dia
            frota=frota_det.loc[frota_det['route_short_name'] == item]
            frota=frota[frota['data']==date.strftime('%Y-%m-%d')]

            #pegar por servico e data e faixa e pegar a maior  para saber frota ops        
            frota_ops_tarde_noite_dia = frota_ops_tarde_noite.loc[frota_ops['servico'] == item]
            frota_ops_tarde_noite_dia = frota_ops_tarde_noite_dia.loc[frota_ops_tarde_noite_dia['data']==date.strftime('%Y-%m-%d')]
         


            #cruza para pegar feriado e dia de operação e decidir se operante ou não
            if not calendar[calendar['route_short_name']==item].empty:
                
                roda_feriado = calendar[calendar['route_short_name']==item]['holiday'].item() =='1' and frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].squeeze()

                if calendar[calendar['route_short_name']==item][date.strftime('%A').lower()].item()=="1" or  roda_feriado:
                    crl+=1
                    
                    if crl <=5: # frota_operante[date].loc[item] == frota_operante[date].loc[item] checks not NaN
                        percent=frota_ops_tarde_noite_dia['frota_aferida'].max()/frota[frota['data']==date.strftime('%Y-%m-%d')]['frota_servico'].squeeze()
                        
                        if percent==percent and type(percent) == np.float64:
                            frota_operante[date].loc[item]=percent
                            
                        else:
                            frota_operante[date].loc[item]=0
                            
                        #verifica domingo  and feriado 
                        #no oficio, 80% da frota det dia util | 40% em dom e feriado --> 0.2|0.1
                        if date.strftime('%A').lower()=="sunday" and frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].squeeze():
                            frota_operante['operante'].loc[item]=frota_operante[date].loc[item] >= 0.1 and frota_operante['operante'].loc[item]
                        else:
                            frota_operante['operante'].loc[item]=frota_operante[date].loc[item] >= 0.2 and frota_operante['operante'].loc[item]
                    
            elif crl<5:
                frota_operante['operante'].loc[item] =False

        if frota_operante['operante'].loc[item] and frota_operante['operante_faixa'].loc[item]=="Sem previsão de funcionamento":
            frota_operante['operante_faixa'].loc[item]="Tarde/Noite"
        elif frota_operante['operante'].loc[item]:#frota_operante['operante_faixa'].loc[item]!="Sem previsão de funcionamento":
            frota_operante['operante_faixa'].loc[item]+=" Tarde/Noite"
        
    

    ######################################################

    
    
    #######################Noturno######################


    # como o periodo é 22 - 4, faremos um união
    frota_ops_noturno=frota_ops.loc[frota_ops['hora'] >= faixa_horaria['Noturno'][0]]  
    frota_ops_noturno= frota_ops_noturno.append(frota_ops_noturno.loc[frota_ops['hora'] < faixa_horaria['Noturno'][1]])
    frota_operante['operante']=True

    for item in linhas:
        crl=0

        for i in range(1,10):#setar range = date_interval como parametro do pipeline # pegar do date.unique
            date = date_now - datetime.timedelta(days=i)

            #pegar a frota det por dia
            frota=frota_det.loc[frota_det['route_short_name'] == item]
            frota=frota[frota['data']==date.strftime('%Y-%m-%d')]
            
            #pegar por servico e data e faixa e pegar a maior  para saber frota ops        
            frota_ops_noturno_dia = frota_ops_noturno.loc[frota_ops['servico'] == item]# <--- jogar isso para fora do for.
            frota_ops_noturno_dia = frota_ops_noturno_dia.loc[frota_ops_noturno_dia['data']==date.strftime('%Y-%m-%d')]

                      

            #cruza para pegar feriado e dia de operação e decidir se operante ou não
            if not calendar[calendar['route_short_name']==item].empty:

                
                #print("parte1 calendar[calendar['route_short_name']==item]['holiday'].squeeze() =='1' ", calendar[calendar['route_short_name']==item]['holiday'].item() =="1", calendar[calendar['route_short_name']==item]['holiday'].item())
                #print("parte2 frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].squeeze() ",frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].item(),frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].item() and True )

                roda_feriado = calendar[calendar['route_short_name']==item]['holiday'].item() =='1' and frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].squeeze()
                
                #print(frota_ops_noturno_dia)
                #print('roda', roda_feriado,"calendar[calendar['route_short_name']==item]['holiday'].item() =='1' " ,calendar[calendar['route_short_name']==item]['holiday'].item() =='1',"crl ", crl, " frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].item()",frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].item())
                if calendar[calendar['route_short_name']==item][date.strftime('%A').lower()].item()=="1" or  roda_feriado:
                    crl+=1
                
                    if crl <=5: # frota_operante[date].loc[item] == frota_operante[date].loc[item] checks not NaN
                        percent=frota_ops_noturno_dia['frota_aferida'].max()/frota[frota['data']==date.strftime('%Y-%m-%d')]['frota_servico'].squeeze()
                        if percent==percent and type(percent) == np.float64:
                            frota_operante[date].loc[item]=percent
                        else:
                            frota_operante[date].loc[item]=0

                        #verifica domingo  and feriado 
                        #no oficio, 80% da frota det dia util | 40% em dom e feriado --> 0.2|0.1
                        if date.strftime('%A').lower()=="sunday" and frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].squeeze():
                            frota_operante['operante'].loc[item]=frota_operante[date].loc[item] >= 0.1 and frota_operante['operante'].loc[item]
                        else:
                            frota_operante['operante'].loc[item]=frota_operante[date].loc[item] >= 0.2 and frota_operante['operante'].loc[item]
                    
            elif crl<5:
                frota_operante['operante'].loc[item] =False

        #exit()
        if frota_operante['operante'].loc[item] and frota_operante['operante_faixa'].loc[item]=="Sem previsão de funcionamento":
            frota_operante['operante_faixa'].loc[item]="Noturno"
        elif frota_operante['operante'].loc[item]:#frota_operante['operante_faixa'].loc[item]!="Sem previsão de funcionamento":
            frota_operante['operante_faixa'].loc[item]+=" Noturno"
        
    
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

    with open("/home/d/code/SMTR/pipelines/pipelines/rj_smtr/projeto_qrcode/"+path_df+".picle", 'rb') as data:
        df = pickle.load(data)
        return df

def save_data(df,df_name):

    with open('/home/d/code/SMTR/pipelines/pipelines/rj_smtr/projeto_qrcode/'+df_name+".picle", 'wb') as output:
        pickle.dump(df, output)

#.
date_now= datetime.datetime(2022,5,1)#datetime.datetime.now()

#criando arquivos de dados
#fops=get_frota_ops()
#save_data(fops,"frota_ops")
#fdet=get_frota_determinada()
#save_data(fdet,"frotdet")
#calend=get_calendar_sspo()
#save_data(calend,"calendar_sppo")

#starts here
frota_ops=open_data(path_df"frota_ops")#get_frota_ops()
frota_det=open_data("frotdet")# get_frota_determinada() 
calendar=open_data("calendar_sppo")#get_calendar_sspo()

print('sanity start\n')
print ( "today is ", date_now)
print ("frota_ops\n",frota_ops)
print ("frota_det\n",frota_det)
print (calendar)
print("frota ops vs det por frota_det['route_short_name']\n",[frota_ops['servico'].iloc[i] in frota_det['route_short_name'].unique() for i in range(len(frota_det['route_short_name'].unique()))])
print("datas \n", frota_det['data'].unique())
print("frota ops vs det por data\n", [(frota_det['data'].unique()[i]==frota_ops['data'].unique()[i]) for i in range(len(frota_det['data'].unique()))])
print('sanity end\n')
#print ('achei route_id ', frota_det[frota_det['route_id'] =='O0439AAA0A'].squeeze() ) #  df['Comedy_Score'].where(df['Rating_Score'] < 50)


#verificar necessidade dessa liha.
#frota_ops = frota_ops[~frota_ops["servico"].str.contains('SV|SN|SP|SR|SE|LECD')]
#.

#faixa=date_now
#faixa_horaria={'manhã': ( faixa.replace(hour=5) ,faixa.replace(hour=11) ), 'Tarde/Noite':( faixa.replace(hour=14),faixa.replace(hour=20) ),'Hora útil':(faixa.replace(hour=5),faixa.replace(hour=20)),'Noturno':(faixa.replace(hour=22),faixa.replace(hour=4))}

faixa_horaria={'Manhã': (5 ,11), 'Tarde/Noite':(14,20 ),'Noturno':(22,4)}

resultado = xptoB(frota_ops,frota_det,calendar,date_now,faixa_horaria)

#print(resultado[resultado["operante_faixa"].str.contains("Tarde", regex=False)]) #print(resultado[resultado["operante_faixa"]=="Noturno"])
#aplicando correção do dict

resultado["operante_faixa"][resultado["operante_faixa"]=="Manhã Tarde/Noite"]="Hora útil"
resultado["operante_faixa"][resultado["operante_faixa"]=="Manhã Tarde/Noite Noturno"]="24h"

#print(resultado)
#exit()



DEFAULT_TIMEOUT = 60
CONFIG_FILENAME = "config.yaml"
ACTIVE_FILENAME = "trip_id_regular.json"
STOP_FIXTURE_FILENAME = "fixtures/stop.json"
OUTPUT_JSON = f"{{model}}.json"

for model in ["agency","linha","route","sequence", "stop" ,"trip"]:
    config = yaml.load(
        open(CONFIG_FILENAME, "r"), 
        Loader=yaml.FullLoader
    )["models"][model]#["route"]#marretado aqui
    result=fetch_sigmob_api(config["source"])

    fixture = []
    for i, record in enumerate(result):
        
        record = parse_data_to_fixture(record, config["json"], i)

        ##### only for trip
        if model=="trips":
            route_short_name=calendar['route_short_name'][calendar["route_id"]==record['fields']["route"]]
            if route_short_name.empty:
                record['fields']["operation_time"]="Sem previsão de funcionamento"
            else:
                faixa=resultado["operante_faixa"].loc[resultado["route_short_name"] == route_short_name.item()]
                if faixa.empty:
                    record['fields']["operation_time"]="Sem previsão de funcionamento"
                else:
                    record['fields']["operation_time"]=faixa.squeeze()


        #if _is_active(record, model):            
        fixture.append(record)



    #with open('data.json', 'w', encoding='utf8') as f:
    #json.dump(fixture, f,indent=4, ensure_ascii=False)#, ensure_ascii=False)

    fname = OUTPUT_JSON.format(model=model)
    with open(fname, "w", encoding='utf8') as f:
        json.dump(fixture, f, indent=4, ensure_ascii=False)
        print("Fixture saved on "+fname)
    #exit()



'''
# pipeline roudup
 ---  refazer os arquivos de dados. <-- done
 --- replicar noturno para demais faixas <-- done
 --- cruzamento do route_id na tabela frota_det com o trips.json  <--done
 --- gerar arquivo json com o generate fixtures em trip. -->done
 --- fazer o controle de modelo do json --> done
 --- organizar e rodar pipeline. <-- doing
'''