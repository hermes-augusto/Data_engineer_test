
hive -e "create external IF NOT EXISTS table db_teste.transacoes_env (NOME string, CPF string, CLASSE string)
STORED AS parquet
location 'hdfs://datalakeit-m/teste/files/parquet/transacao_enviada_202012.snappy.parquet';
msck repair table db_teste.transacoes_env;"

hive -e "create external  table IF NOT EXISTS db_teste.transacoes_rec (NOME string, Destino string, VALOR_RECEBIDO string, CHAVE_RECEBIDA string)
STORED AS parquet
location 'hdfs://datalakeit-m/teste/files/parquet/transacao_recbida_202012.snappy.parquet';
msck repair table db_teste.transacoes_rec;"

hive -e "create external  table IF NOT EXISTS db_teste.alertas (Chave_recebida string, CLASSE string, soma double, qtd bigint, Alerta string)
STORED AS parquet
location 'hdfs://datalakeit-m/teste/files/parquet/base_alertas_202012.snappy.parquet';
msck repair table db_teste.alertas;"
hive -e ""
