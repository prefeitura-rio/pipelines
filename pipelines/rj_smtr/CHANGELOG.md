# Changelog - rj_smtr

## [1.0.0] - 2024-05-21

### Alterado

- Inclui tratamento específico na task `transform_raw_to_nested_structure` relacionado aos flows `br_rj_riodejaneiro_gtfs` (https://github.com/prefeitura-rio/pipelines/pull/687)
- Altera constantes e parâmetros de captura e materialização relacionados aos flows `br_rj_riodejaneiro_gtfs` (https://github.com/prefeitura-rio/pipelines/pull/687)

### Corrigido
- Corrige erro `pipelines/rj_smtr/tasks.py:116:64: E226 missing whitespace around arithmetic operator` na task `build_incremental_model` (https://github.com/prefeitura-rio/pipelines/pull/687)