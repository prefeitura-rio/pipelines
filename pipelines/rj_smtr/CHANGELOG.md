# Changelog - rj_smtr

## [1.0.2] - 2024-06-03

### Corrigido

- Altera label de DEV para PROD no schedule `every_friday_seven_thirty`


## [1.0.1] - 2024-05-22

### Alterado

- Altera `primary_key` da constante `GTFS_TABLE_CAPTURE_PARAMS` relativa a `ordem_servico_trajeto_alternativo` (https://github.com/prefeitura-rio/pipelines/pull/690)

## [1.0.0] - 2024-05-21

### Alterado

- Inclui tratamento específico na task `transform_raw_to_nested_structure` relacionado aos flows `br_rj_riodejaneiro_gtfs` (https://github.com/prefeitura-rio/pipelines/pull/687)
- Altera constantes e parâmetros de captura e materialização relacionados aos flows `br_rj_riodejaneiro_gtfs` (https://github.com/prefeitura-rio/pipelines/pull/687)

### Corrigido

- Corrige erro `pipelines/rj_smtr/tasks.py:116:64: E226 missing whitespace around arithmetic operator` na task `build_incremental_model` (https://github.com/prefeitura-rio/pipelines/pull/687)