# Changelog - br_rj_riodejaneiro_gtfs

## [1.0.1] - 2024-05-10

### Alterado

- Cria parâmetro `project_name` no flow `gtfs_captura_tratamento` para possibilitar alterar o `project_name` ao usar a task `create_flow_run` (https://github.com/prefeitura-rio/pipelines/pull/678)
- Inclui argumento `run_config` ao usar a task `create_flow_run`, usando sempre como referência a `run_config` do flow principal (https://github.com/prefeitura-rio/pipelines/pull/678)

## [1.0.0] - 2024-04-18

### Adicionado

- Adiciona nova tabela de `ordem_servico_trajeto_alternativo` na captura e tratamento (https://github.com/prefeitura-rio/pipelines/pull/656)

### Alterado

- Altera parâmetros de materialização para otimização do flow `gtfs_captura_tratamento` (https://github.com/prefeitura-rio/pipelines/pull/656)