# Changelog - veiculo

## [1.0.1] - 2024-05-28

### Adicionado

- Adiciona retry a task `get_raw_ftp` para mitigar as falhas na captura (https://github.com/prefeitura-rio/pipelines/pull/694)

## [1.0.0] - 2024-04-25

### Alterado

- Desliga schedule dos flows `sppo_infracao_captura` e `sppo_licenciamento_captura` em razão de indisponibilidade e geração de dados imprecisos na fonte (SIURB) (https://github.com/prefeitura-rio/pipelines/pull/672)