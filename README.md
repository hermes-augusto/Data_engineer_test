

# Projeto Teste Data Engineer
____________________________________________________
Projeto para ingestão de dados em Python.


# Solução

![alt text](https://github.com/VitorPaes/BoaVista_TST/blob/main/_exp.png?raw=true)

A solução consiste em armazenar os dados disponibilizados (formato .csv) em um bucket  Google Storage, carregá-los por meio do framework Pyspark pelo cluster Dataproc convertendo-o para o formato colunar parquet disponibilizados no mesmo ambiente. Após a ingesta dos dados os mesmo ficam disponíveis na ferramenta Big Query podendo extrair relatório com o Data Studio.  


## Execução

Fazer Login no servidor enviado e executar o comando:

```bash
cd scripts 
./00_create_tables.sh "/home/boavista/scripts"
```
