

# Projeto Teste Data Engineer

Projeto para ingestão de dados em Python.


# Solução

![alt text](https://github.com/hermes-augusto/Data_engineer_test/blob/main/_exp.jpeg)

A solução consiste em armazenar os dados disponibilizados  em um bucket  Google Storage, carregá-los por meio do framework Pyspark pelo cluster Dataproc convertendo-o para o formato colunar parquet disponibilizados no mesmo ambiente. 

## Execução

Fazer Login no servidor enviado e executar o comando:
Para realizar a geração da massa de teste, ingesta dessa massa, ETL e gerar as bases  no HIVE
sh /sistema/empresas/itau/scripts/main.sh 100
Apos se quiser analisar a base gerar no hive utilize tabelas: db_teste.transacoes_env(transações enviadas), db_teste.transacoes_rec (transações recebidas), db_teste.alertas (Base com alertas gerados de possiveis fraudes) 
 beeline -u "jdbc:hive2://localhost:10000" 
 QUERY
>```<seu códigto aqui
```
