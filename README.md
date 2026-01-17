# TccUspEsalq
Repositório para armazenar os dados do meu projeto de conclusão de curso da usp esalq.

## Objetivo do projeto
Comparar o processamento da biblioteca pandas com o framework spark usando diferentes tamanhos de arquivos gerados utilizando a técnica de benchmark tpch.


## Passo a passo de execução:
* Primeiro temos que gerar os arquivos TPCH (formato tbl):
> docker compose build tpchgen
> 
> docker compose up tpchgen

* Segundo passo é converter os arquivos formato tbl em arquivos parquet:
> docker compose build parquet
> 
> docker compose up parquet

* Terceiro passo é executar o benchmark do framework spark (pyspark):
> docker compose build spark
> 
> docker compose up spark

* Último passo é executar o benchmark a biblioteca pandas:
> docker compose build pandas
> 
> docker compose up pandas
