# Pipelines

Esse repositório contém flows desenvolvidos com Prefect relacionados ao Escritório Municipal de Dados da Prefeitura do Rio de Janeiro.

## Como desenvolver

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
Links para a documentação do Prefect também encontram-se nos comentários.
