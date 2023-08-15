import urllib.request

#urllib.request.urlretrieve('ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_202307.ZIP',
#                            '~\BASE_DE_DADOS_CNES_202307.ZIP')

import shutil
import urllib.request
from contextlib import closing

with closing(urllib.request.urlopen('ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_202307.ZIP')) as r:
    with open('~\BASE_DE_DADOS_CNES_202307.ZIP', 'wb') as f:
        shutil.copyfileobj(r, f)


def get_file_size(url):
    try:
        response = urllib.request.urlopen(url)
        file_size = response.headers.get("Content-Length")
        
        if file_size:
            file_size = int(file_size)
            return file_size
        else:
            return None  # Content-Length header not found
        
    except Exception as e:
        print(f"Error: {e}")
        return None

url = "ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_202307.ZIP"
file_size = get_file_size(url)

if file_size is not None:
    print(f"File size: {file_size} bytes")
else:
    print("Unable to retrieve file size.")