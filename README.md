# Pipelines

Este repositório contém fluxos de captura e subida de dados no datalake
da Prefeitura do Rio de Janeiro. O repositório é gerido pelo Escritório
Municipal de Dados (EMD) e alimentado de forma colaborativa com as equipes de
dados e tecnologia das Secretarias.

> 💜 Todo o código é desenvolvido em Python utilizando o software livre [Prefect](https://prefect.io/).

## Prepare o ambiente

### Requrimentos

- Python 3.6+
- Poetry

### Instalação

```sh
python -m venv .venv # recomendamos que use um ambiente virtual
source .venv/bin/activate # ative o ambiente
poetry install # instale os requisitos do projeto
```

## Como adicionar seu órgão

As pipelines dos diferentes órgãos funcionam de forma independente,
porém todo o código é registrado neste repositório.

O código é separado da seguinte forma:

```
├── pipelines              <- (0) Esta é a pasta onde estão os códigos das pipelines
│   ├── emd                <- (1) Cada órgão tem uma pasta com seus códigos (esta é do EMD)
│   ├── cor                <- Esta por exemplo é a pasta do Centro de Operações (COR)
│   ├── smtr               <- E esta é da Secretaria Municipal de Transportes (SMTR)
│   └── ...                <- (2) Outros órgãos podem adicionar aqui suas pastas, basta...
├── manage.py              <- (3) Este arquivo possui comandos auxiliares
```

**Para criar a pasta do seu órgão, basta rodar o comando abaixo:**

```sh
python manage.py add-project <sigla do órgão em snakecase*>
```

*snakecase: sem acentos, minúsculo e sem espaços.

Após rodar, sua pasta deve aparecer em [`pipelines/`](/pipelines/).

Para listar os órgãos e nomes registrados, basta rodar:

```sh
python manage.py list-projects
```

## Como criar sua 1a pipeline

Uma vez criada a pasta do seu órgão, é possível construir e testar
pipelines (**flows**).

### Configuração

1. Crie a pipeline na pasta do seu órgão
   `pipelines/<orgao>/<nova_pipeline>`. Caso seja a captura de uma
   base a pasta deve ser o nome do `dataset_id`.

2. Copie os arquivos de
   [template_pipeline](/pipelines/emd/template_pipeline/) para a pasta
   criada.

> O que são esses arquivos? O Prefect utiliza arquivos específicos
> chamados de `flows.py` (conjunto/fluxo de ações), `tasks.py` (ações) e
> `schedules.py` (rotinas) na construção de pipelines. [Leia mais aqui](https://docs.prefect.io/core/concepts/tasks.html).

3. Edite o arquivo `pipelines/<orgao>/__init__.py`, registrando sua nova
   pipeline:

```py
from .<nova_pipeline>.flows import *
```

4. Ao final da configuração, a estrutura obtida deve ser a seguinte:

```
├── pipelines
│   └── <orgao>                <- Pasta do seu órgão
│       ├── __init__.py        <- Arquivo editado em no Passo 3
│       └── <nova_pipeline>    <- Pasta da sua nova pipeline
│           ├── __init__.py    <- Arquivo vazio
│           ├── flows.py       <- Arquivo onde será escrito o fluxo de ações
│           ├── schedules.py   <- Arquivo onde serão escritas as rotinas (quando a pipeline irá rodar)
│           └── tasks.py       <- Arquivo onde serão escritas as ações (funções)
```

### Construindo a pipeline

1. Comece pelo arquivo `pipelines/<orgao>/<nova_pipeline>/flows.py`, declarando o fluxo que será executado
   pela pipeline assim como chamando as funções (tasks) que serão
   executadas. No próopio arquivo você encontra instruções específicas
   de como escrevê-lo.

> As funções não precisam ter sido criadas ainda em
   `tasks.py`, recomendamos que pense nos passos (inputs e outputs
   necessários de cada um) e depois
   escreva as funções em si.

2. Após escrever o fluxo, crie as funções necessárias para sua execução
   em `pipelines/<orgao>/<nova_pipeline>/tasks.py`. Este arquivo também
   possui instruções de como escrevê-lo.

> Caso necessário, crie um arquivo `pipelines/<orgao>/constants.py` ([exemplo](/pipelines/constants.py)) para armazenar constantes que serão utilizadas nas `tasks`.

## Testando sua pipeline

### Local

1. Crie o arquivo `test.py` importanto a pipeline que deseja executar e adicione a função `run_local`
com os parâmetros necessários:

```py
from pipelines.emd.utils import run_local
from pipelines.emd.test_flow.flows import flow

run_local(flow, parameters = {"param": "val"})
```

3. Rode a pipeline localmente com:

```sh
python pipelines/test.py
```

### Na nuvem

1. Configure as variáveis de ambiente num arquivo chamado `.env` na raiz
   do projeto:

```.env
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json  # Credenciais do Google Cloud
PREFECT__BACKEND=server
PREFECT__SERVER__HOST=http://prefect-apollo.prefect.svc.cluster.local
PREFECT__SERVER__PORT=4200
```

> Atenção: `GOOGLE_APPLICATION_CREDENTIALS` deve conter a credencial de uma conta de
> serviço com (no mínimo) acesso de **escrita** ao bucket datario-public no Google
> Cloud Storage.

2. Crie o arquivo `test.py` com a pipeline que deseja executar e adicione a função `run_cloud`
com os parâmetros necessários:

```py
from pipelines.utils import run_cloud
from pipelines.[secretaria].[pipeline].flows import flow # Complete com as infos da sua pipeline

run_cloud(
    flow,               # O flow que você deseja executar
    labels=[
        "example",      # Label para identificar o agente que irá executar a pipeline (ex: rj-sme)
    ],
    parameters = {
        "param": "val", # Parâmetros que serão passados para a pipeline (opcional)
    }
)
```

3. Rode a pipeline localmente com:

```sh
python pipelines/test.py
```

A saída deve se assemelhar ao exemplo abaixo:

```sh
[2022-02-19 12:22:57-0300] INFO - prefect.GCS | Uploading xxxxxxxx-development/2022-02-19t15-22-57-694759-00-00 to datario-public
Flow URL: http://localhost:8080/default/flow/xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
 └── ID: xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
 └── Project: main
 └── Labels: []
Run submitted, please check it at:
http://prefect-ui.prefect.svc.cluster.local:8080/flow-run/xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

> ATENÇÃO : Tenha certeza que você possui acesso à interface do Prefect
> [neste link](http://prefect-ui.prefect.svc.cluster.local:8080/). Você
> precisa do acesso tanto para submeter a run como para acompanhá-la durante a execução. Caso não tenha, verifique o procedimento em [aqui](https://library-emd.herokuapp.com/infraestrutura/como-acessar-a-ui-do-prefect).

4. (**Recomendado**) Quando acabar de desenvolver sua pipeline, delete
   todas as versõs teste da mesma na interface do Prefect.

<!-- ## Como desenvolver

O script `manage.py` é responsável por criar e listar projetos desse repositório. Para usá-lo, no entanto, você deve instalar as dependências em `requirements-cli.txt`. Você pode obter mais informações sobre os comandos

```
python manage.py --help
```

O comando `add-project` permite que você crie um novo projeto a partir do template padrão. Para criar um novo projeto, basta fazer

```
python manage.py add-project nome-do-projeto
```

Isso irá criar um novo diretório com o nome `nome-do-projeto` em `pipelines/` com o template padrão, já adaptado ao nome do projeto. O nome do projeto deve estar em [snake case](https://en.wikipedia.org/wiki/Snake_case) e deve ser único. Qualquer conflito com um projeto já existente será reportado.

Para listar os projetos existentes e nomes reservados, basta fazer

```
python manage.py list-projects
```

Em seguida, leia com anteção os comentários em cada um dos arquivos do seu projeto, de modo a evitar conflitos e erros.
Links para a documentação do Prefect também encontram-se nos comentários. -->
