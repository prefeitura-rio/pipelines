import requests


#@task()
def get_service_sigmod()-> Tuple[str, int]:#alterar antes de testar
    """
        Returns service data from SIGMOB api 
    """

    url ="https://jeap.rio.rj.gov.br/MOB/get_routes.rule?sys=MOB"   #colocar como parametro 
    r = requests.get(url)
    data= r.json()["result"]
    #data = SV_SN_Filter(r.json)

    return data

#@task()
def filter_by_tag(data):
    """
        Returns SIGMOB response without SN SV tagged services
    """
    services_list=[]

        for item in data: 
            
            if not re.search(r'SV|SN\b', item["SiglaServico"]):
            
                services_list.append(item)

    return services_list