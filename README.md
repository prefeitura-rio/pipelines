# Pipelines

Este repositório contém fluxos de captura e subida de dados no datalake
da Prefeitura do Rio de Janeiro. O repositório é gerido pelo Escritório
Municipal de Dados (EMD) e alimentado de forma colaborativa com as equipes de
dados e tecnologia das Secretarias.

> 💜 Todo o código é desenvolvido em Python utilizando o software livre [Prefect](https://prefect.io/).

## Configuração de ambiente para desenvolvimento

### Requisitos

- Um editor de texto (recomendado VS Code)
- Python 3.9.x
- `pip`
- (Opcional, mas recomendado) Um ambiente virtual para desenvolvimento (`miniconda`, `virtualenv` ou similares)

### Procedimentos

- Clonar esse repositório

```
git clone https://github.com/prefeitura-rio/pipelines
```

- Abrí-lo no seu editor de texto

- No seu ambiente de desenvolvimento, instalar [poetry](https://python-poetry.org/) para gerenciamento de dependências

```
pip3 install poetry
```

- Instalar as dependências para desenvolvimento

```
poetry install
```

- Instalar os hooks de pré-commit (ver [#127](https://github.com/prefeitura-rio/pipelines/pull/127) para entendimento dos hooks)

```
pre-commit install
```

- Pronto! Seu ambiente está configurado para desenvolvimento.

---

## Como desenvolver

### Estrutura de diretorios

```
orgao/                       # diretório raiz para o órgão
|-- projeto1/                # diretório de projeto
|-- |-- __init__.py          # vazio
|-- |-- constants.py         # valores constantes para o projeto
|-- |-- flows.py             # declaração dos flows
|-- |-- schedules.py         # declaração dos schedules
|-- |-- tasks.py             # declaração das tasks
|-- |-- utils.py             # funções auxiliares para o projeto
...
|-- __init__.py              # importa todos os flows de todos os projetos
|-- constants.py             # valores constantes para o órgão
|-- flows.py                 # declaração de flows genéricos do órgão
|-- schedules.py             # declaração de schedules genéricos do órgão
|-- tasks.py                 # declaração de tasks genéricas do órgão
|-- utils.py                 # funções auxiliares para o órgão

orgao2/
...

utils/
|-- __init__.py
|-- flow1/
|-- |-- __init__.py
|-- |-- flows.py
|-- |-- tasks.py
|-- |-- utils.py
|-- flows.py                 # declaração de flows genéricos
|-- tasks.py                 # declaração de tasks genéricas
|-- utils.py                 # funções auxiliares

constants.py                 # valores constantes para todos os órgãos

```

### Adicionando órgãos e projetos

O script `manage.py` é responsável por criar e listar projetos desse repositório. Para usá-lo, no entanto, você deve instalar as dependências em `requirements-cli.txt`:

```
pip3 install -r requirements-cli.txt
```

Você pode obter mais informações sobre os comandos com

```
python manage.py --help
```

O comando `add-agency` permite que você adicione um novo órgão a partir do template padrão. Para fazê-lo, basta executar

```
python manage.py add-agency nome-do-orgao
```

Isso irá criar um novo diretório com o nome `nome-do-orgao` em `pipelines/` com o template padrão, já adaptado ao nome do órgão. O nome do órgão deve estar em [snake case](https://en.wikipedia.org/wiki/Snake_case) e deve ser único. Qualquer conflito com um projeto já existente será reportado.

Para listar os órgão existentes e nomes reservados, basta fazer

```
python manage.py list-projects
```

Em seguida, leia com anteção os comentários em cada um dos arquivos do seu projeto, de modo a evitar conflitos e erros.
Links para a documentação do Prefect também encontram-se nos comentários.

Caso o órgão para o qual você desenvolverá um projeto já exista, basta fazer

```
python manage.py add-project nome-do-orgao nome-do-projeto
```

### Adicionando dependências para execução

- Requisitos de pipelines devem ser adicionados com

```
poetry add <package>
```

- Requisitos do `manage.py` estão em `requirements-cli.txt`

- Requisitos para a Action de deployment estão em `requirements-deploy.txt`

- Requisitos para testes estão em `requirements-tests.txt`

### Como testar uma pipeline localmente

Escolha a pipeline que deseja executar (exemplo `pipelines.rj_escritorio.template_pipeline.flows.flow`)

```py
from pipelines.utils.utils import run_local
pipelines.rj_escritorio.template_pipeline.flows import flow

run_local(flow, parameters = {"param": "val"})
```

### Como testar uma pipeline na nuvem

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

- `VAULT_ADDRESS`: deve ter o valor `http://vault.vault.svc.cluster.local:8200/`

- `VAULT_TOKEN`: deve ter o valor do token do órgão para o qual você está desenvolvendo. Caso não saiba o token, entre em contato.

- Em seguida, tenha certeza que você já tem acesso à UI do Prefect, tanto para realizar a submissão da run, como para
  acompanhá-la durante o processo de execução. Caso não tenha, verifique o procedimento em https://library-emd.herokuapp.com/infraestrutura/como-acessar-a-ui-do-prefect

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

- (Opcional, mas recomendado) Quando acabar de desenvolver sua pipeline, delete todas as versões da mesma pela UI do Prefect.
