import os
os.environ["HADOOP_HOME"] = "C:\\Hadoop"
os.environ["SPARK_HOME"] = "C:\\spark"

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Comparison").getOrCreate()

#função para ler o arquivo clientes.csv e retornar um DataFrame
def ler_arquivo_clientes():
    df = spark.read.csv("./atividade1/clientes.csv", header=True, inferSchema=True, encoding="utf-8")
    return df

#função para filtrar clientes com idade maior que 30 anos
def filtrar_clientes_maior_que_30(df):
    df = df.filter(df["idade"] > 30)
    return df 

#função para contar clientes que da cidade de São Paulo
def contar_clientes_sao_paulo(df):    
    df = df.filter(df["cidade"] == "São Paulo").count()
    return df

#Lógica principal
df = ler_arquivo_clientes()
df.show(5)
df = filtrar_clientes_maior_que_30(df)
df.show(5)
df = contar_clientes_sao_paulo(df)
print(f"Total de clientes da cidade de São Paulo: {df}")