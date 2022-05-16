import datetime
import requests

import basedosdados as bd
import json
import numpy as np
import pandas as pd
import yaml

#,
import pickle


@task
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
    
@task
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

@task
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
   
    query += "DATE(f1.data_versao) = d.data_versao_efetiva_routes and d.data = date('{}')) f where f.route_short_name=c.route_short_name and c.{} ='1' and f.route_id is not null".format(date_now.strftime('%Y-%m-%d'),date_now.strftime('%A').lower())

    sppo_calendar_today = get_data(query)
    # ~ marca um operador NOT
    sppo_calendar_today = sppo_calendar_today[~sppo_calendar_today["route_short_name"].str.contains('SV|SN|SP|SR|SE|LECD')]
    return sppo_calendar_today

@task
def xptoB(frota_ops,frota_det,calendar,date_now,faixa_horaria)->pd.DataFrame:
    """
    Returns dataframe regarding operation of the last 5 days.
    """

    datas = frota_det['data'].unique() #formatar data
    linhas = frota_det['route_short_name'].unique()
    frota_operante=pd.DataFrame([],index=linhas,columns=datas)
    frota_operante['route_short_name']=linhas
    frota_operante['operante_faixa']="Sem previsão de funcionamento"      
    

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
                    
                    if crl <=5: 
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
        elif frota_operante['operante'].loc[item]:
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
                    
                    if crl <=5: 
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
        elif frota_operante['operante'].loc[item]
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

                
                roda_feriado = calendar[calendar['route_short_name']==item]['holiday'].item() =='1' and frota[frota['data']==date.strftime('%Y-%m-%d')]['data_feriado'].squeeze()

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

        
        if frota_operante['operante'].loc[item] and frota_operante['operante_faixa'].loc[item]=="Sem previsão de funcionamento":
            frota_operante['operante_faixa'].loc[item]="Noturno"
        elif frota_operante['operante'].loc[item]
            frota_operante['operante_faixa'].loc[item]+=" Noturno"
        
    
    ######################################################
    
    frota_operante["operante_faixa"][frota_operante["operante_faixa"]=="Manhã Tarde/Noite"]="Hora útil"
    frota_operante["operante_faixa"][frota_operante["operante_faixa"]=="Manhã Tarde/Noite Noturno"]="24h"
    
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




def get_date_range(date_interval) -> (str,str):#setar date_interval como parametro do pipeline
    """
    Returns tuple of date strings (10 days before today, today)
    """
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=date_interval)#pega 10 dias pq não sabemos os feriados.
    return (start_date.strftime('%Y-%m-%d'),end_date.strftime('%Y-%m-%d'))

def get_data(query) -> pd.DataFrame:
    """
    Returns the dataframe from basedosdados.
    """
    
    return bd.read_sql(query=query, billing_project_id="rj-smtr-dev", from_file=True)



#test onky
@task
def open_data(path_df):

    with open("/home/d/code/SMTR/pipelines/pipelines/rj_smtr/projeto_qrcode/"+path_df+".picle", 'rb') as data:
        df = pickle.load(data)
        return df

