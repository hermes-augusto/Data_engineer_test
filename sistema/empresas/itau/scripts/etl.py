# -*- coding: utf-8 -*-
from pyspark.sql.functions import sum, col, count,when
import datetime
from pyspark import SparkConf
from pyspark.sql import SparkSession
import env_mail
spark = SparkSession.builder.appName("teste_itau").getOrCreate()

print(str(datetime.datetime.now())+': Realiza leitura das bases em CSV')
df1 = spark.read.csv('hdfs://datalakeit-m/teste/files/massa_teste.csv',header=True,sep=',')
df1 = df1.dropDuplicates(['CPF'])
df2  = spark.read.csv('hdfs://datalakeit-m/teste/files/massa_rec.csv',header=True,sep=',').withColumnRenamed('CPF','Destino')

print(str(datetime.datetime.now())+': Realiza escritas das bases em parquet')
df1.write.option("compression", "snappy").mode("overwrite").parquet('hdfs://datalakeit-m/teste/files/parquet/transacao_enviada_202012.snappy.parquet')
df2.write.option("compression", "snappy").mode("overwrite").parquet('hdfs://datalakeit-m/teste/files/parquet/transacao_recbida_202012.snappy.parquet')
print(str(datetime.datetime.now())+': Realiza operacoes de cruzamento + agregação e validaçao')

df3 = df1.alias('A').join(df2.alias('B'),df1['CPF'] == df2['CHAVE_RECEBIDA'], 'left').select('A.CPF','A.CLASSE','B.Destino','B.VALOR_RECEBIDO','B.CHAVE_RECEBIDA').filter(col('CHAVE_RECEBIDA').isNotNull())

x = df3.groupBy('Chave_recebida','CLASSE').agg(sum('valor_recebido').alias('soma'),count('CHAVE_RECEBIDA').alias('qtd'))
xy = x.withColumn('Alerta',when((col('CLASSE')== 'uniclass') &  (col('qtd')>=3) & (col('soma')>50000),'Fraude')\
	.when((col('CLASSE')== 'personalite') &  (col('qtd')>=3) & (col('soma')>75000),'Fraude')\
	.when((col('CLASSE')== 'privete') &  (col('qtd')>=3) & (col('soma')>100000),'Fraude').otherwise('OK'))
print(str(datetime.datetime.now())+': Gera base para validaçao do backoffice')
xy.write.option("compression", "snappy").mode("overwrite").parquet('hdfs://datalakeit-m/teste/files/parquet/base_alertas_202012.snappy.parquet')

if env_mail.valida_email(xy):
    env_mail.envia_mail()
else:
    print(str(datetime.datetime.now())+'Tudo certo!')
