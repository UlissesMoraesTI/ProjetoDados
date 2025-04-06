import os
os.environ["HADOOP_HOME"] = "C:\\Hadoop"
os.environ["SPARK_HOME"] = "C:\\spark"

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col


spark = SparkSession.builder.appName("Comparison").getOrCreate()

#função para ler o arquivo clientes.csv e retornar um DataFrame
def ler_arquivo_clientes():
    df_cli = spark.read.csv("./atividade1/clientes.csv", header=True, inferSchema=True, encoding="utf-8")
    return df_cli

#função para renomear a coluna id_cliente para id_cliente_cli
def renomear_coluna_id_cliente_cli(df_cli):
    df_cli = df_cli.withColumnRenamed("id_cliente", "id_cliente_cli")  # Renomeando a coluna
    return df_cli

#função para ler o arquivo clientes.csv e retornar um DataFrame
def ler_arquivo_transacoes():
    df_trn = spark.read.csv("./atividade2/transacoes.csv", header=True, inferSchema=True, encoding="utf-8")
    return df_trn

#função para renomear a coluna id_cliente para id_cliente_trn
def renomear_coluna_id_cliente_trn(df_trn):
    df_trn = df_trn.withColumnRenamed("id_cliente", "id_cliente_trn")  # Renomeando a coluna
    return df_trn

#função join para juntar os DataFrames de clientes e transações
def juntar_dataframes(df_cli, df_trn):
    df = df_cli.join(df_trn, col("id_cliente_cli") == col("id_cliente_trn"), "inner") 
    return df

#função para agrupar por cliente criar uma nova coluna total_transacoes que represente o valor total de transações por cliente
def  agrupar_por_cliente(df):
    df = df.groupBy("id_cliente_cli").agg(sum("valor").alias("total_transacoes"))
    return df
#função para mostrar o cliente com maior valor total de transações
def cliente_maior_transacao(df):
    df = df.orderBy(col("total_transacoes").desc()).limit(1)  # Ordena e pega o maior
    return df 

#Lógica principal
df_cli = ler_arquivo_clientes()
df_cli.show(5)
df_cli = renomear_coluna_id_cliente_cli(df_cli)
df_cli.show(5)
df_trn = ler_arquivo_transacoes()
df_trn.show(5)
df_trn = renomear_coluna_id_cliente_trn(df_trn)
df_trn.show(5)
df = juntar_dataframes(df_cli, df_trn)
df.show(5)
df = agrupar_por_cliente(df)
df.show(5)
df = cliente_maior_transacao(df)
df.show(5)