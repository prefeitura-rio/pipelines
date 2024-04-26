# Changelog - br_rj_riodejaneiro_onibus_gps

## [1.0.1] - 2024-04-26

### Adicionado

- Cria task `clean_br_rj_riodejaneiro_onibus_gps` (https://github.com/prefeitura-rio/pipelines/pull/673)

### Alterado

- Otimiza e inclui parâmetros de rematerialização no flow `materialize_sppo` (https://github.com/prefeitura-rio/pipelines/pull/673)

## [1.0.0] - 2024-04-26

### Adicionado

- Adiciona flow `recaptura_realocacao_sppo` (https://github.com/prefeitura-rio/pipelines/pull/668)

### Alterado

- Altera flow `recaptura`, incluindo acionamento do `recaptura_realocacao_sppo` (https://github.com/prefeitura-rio/pipelines/pull/668)

### Corrigido

- Corrigido parâmetro `timestamp` do flow `realocacao_sppo` (https://github.com/prefeitura-rio/pipelines/pull/668)
