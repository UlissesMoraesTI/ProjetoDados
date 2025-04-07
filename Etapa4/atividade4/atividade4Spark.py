import os
os.environ["HADOOP_HOME"] = "C:\\Hadoop"
os.environ["SPARK_HOME"] = "C:\\spark"

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

spark = SparkSession.builder.appName("Comparison").getOrCreate()

#função para ler o arquivo clientes e retornar um DataFrame
def ler_arquivo_clientes():
    df = spark.read.csv("./atividade4/clientes.csv", header=True, inferSchema=True, encoding="utf-8")
    return df

#função para filtrar clientes com mais de 30 anos 
def filtrar_clientes_maior_30(df):
    df_maior_30 = df.filter(col("idade") > 30)
    return df_maior_30

#função para ler o arquivo transações e retornar um DataFrame
def ler_arquivo_transacoes():
    df = spark.read.csv("./atividade4/transacoes.csv", header=True, inferSchema=True, encoding="utf-8")
    return df

#função para agrupar as transações por categoria e quantidade, calculanado o valor total(valor_total)
def agrupar_transacoes(df):
    df = df.groupBy("categoria").agg(
        sum(col("valor")).alias("valor_total"))
    return df

#Lógica principal
df = ler_arquivo_clientes()
df.show(5)
df = filtrar_clientes_maior_30(df)
df.show(5)
df = ler_arquivo_transacoes()
df.show(5)
df = agrupar_transacoes(df)
df.show(5)

#Parecer conclusivo no comparativo entre Pandas e Spark: No conceito lógica de programação 
# são identicos, o código Pandas é mais simples de entender e mais fácil de implementar,
# e roda absurdamente mais rápido.#   