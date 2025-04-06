import os
os.environ["HADOOP_HOME"] = "C:\\Hadoop"
os.environ["SPARK_HOME"] = "C:\\spark"

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Comparison").getOrCreate()

#função para ler o arquivo clientes.csv e retornar um DataFrame
def ler_arquivo_transacoes():
    df = spark.read.csv("./atividade2/transacoes.csv", header=True, inferSchema=True, encoding="utf-8")
    return df

#função extrair o ano da data da transação e criar uma nova coluna "ano_transacao"
def extrair_ano_transacao(df):
    df = df.withColumn("ano_transacao", df["data_transacao"].substr(1, 4))
    return df

#função para incluir coluna de quantidade com valor fixo 1
def incluir_coluna_quantidade(df):
    df = df.withColumn("quantidade", lit(1))
    return df

#função para agrupar as transações por categora e quantidade, calculanado o valor total(valor_total)
def agrupar_transacoes(df):
    df = df.groupBy("categoria").agg(
        sum(col("valor")).alias("valor_total"),
        sum(col("quantidade")).alias("quantidade_total"))        
    return df

#função para calcular a média do valor total por categoria dividindo o valor total pela quantidade total 
def calcular_media_por_categoria(df):
    df = df.withColumn("valor_medio", when(col("quantidade_total") == 0, None).otherwise(col("valor_total") / col("quantidade_total")))
    return df

#função para excluir a coluna quantidade
def excluir_coluna_quantidade(df):    
    df = df.drop("quantidade_total")
    return df

#função para classificar DataFrame pela coluna valor_total DESC
def classificar_por_valor_total(df):
    df = df.orderBy(col("valor_total").desc())
    return df

#Lógica principal
df = ler_arquivo_transacoes()
df.show(5)
df = extrair_ano_transacao(df)
df.show(5)
df = incluir_coluna_quantidade(df)
df.show(5)     
df = agrupar_transacoes(df)
df.show(5)
df = calcular_media_por_categoria(df)
df.show(5)
df = excluir_coluna_quantidade(df)    
df.show(5)
df = classificar_por_valor_total(df)
df.show(5)