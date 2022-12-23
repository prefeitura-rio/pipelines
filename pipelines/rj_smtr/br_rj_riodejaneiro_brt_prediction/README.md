# README

Essa pipeline utiliza dados do bucket do gcp, para rodar local precisa ter as credenciais salvas localmente.
https://cloud.google.com/docs/authentication/application-default-credentials#personal

Essa pipeline requer alguns requirements:
- fastparquet
- gcsfs
- haversine

Essa pipeline usa alguns dados estaticos locais que nao sobem para o repositorio. Adicione manualmente
- dados/intervalo_trechos.parquet

