#!/bin/bash
qtd=$1
echo ">>>"
echo ">>>======== Incia processamento  =========="                                                                     
echo ">>> $now >>>Definindo application name como teste_itau"                                    
echo ">>> "
echo ">>> $noew >>> Inicia geração de massa de teste com quantidade de linhas passadas"
python /home/hermes/teste/scripts/gerar_bases.py $qtd
echo ">>> $now >>>Finaliza geração de massas"
echo ">>> $now >>>Inicia ingesta no HDFS    "
hdfs dfs -put -f /home/hermes/teste/files/*.csv hdfs://datalakeit-m/teste/files/
echo ">>> $now >>>Finaliza ingesta no HDFS    "
echo ">>> $now >>> Inicia processamento spark!"
spark-submit /home/hermes/teste/scripts/etl.py
echo ">>> $now >>> Gerando Basese no Hive!"
hive -e "create external table IF NOT EXISTS db_teste.transacoes_env (NOME string, CPF string, CLASSE string)
STORED AS parquet
location 'hdfs://datalakeit-m/teste/files/parquet/transacao_enviada_202012.snappy.parquet';
msck repair table db_teste.transacoes_env;"
echo ">>> $now >>>Finaliza uma   "
hive -e "create external table IF NOT EXISTS db_teste.transacoes_rec (NOME string, Destino string, VALOR_RECEBIDO string, CHAVE_RECEBIDA string)
STORED AS parquet
location 'hdfs://datalakeit-m/teste/files/parquet/transacao_recbida_202012.snappy.parquet';
msck repair table db_teste.transacoes_rec;"
echo ">>> $now >>>Finaliza segunda   "
hive -e "create external table IF NOT EXISTS db_teste.alertas (Chave_recebida string, CLASSE string, soma double, qtd bigint, Alerta string)
STORED AS parquet
location 'hdfs://datalakeit-m/teste/files/parquet/base_alertas_202012.snappy.parquet';
msck repair table db_teste.alertas;"
echo ">>> $now >>>Finalizado o processo!  "
exit 0