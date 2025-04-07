import pandas as pd

#função que leia o arquivo clientes e retorne um DataFrame.
def ler_arquivo_clientes():
    df = pd.read_csv("./atividade4/clientes.csv", sep=None, engine="python")    
    return df

#função filtro idade superior a 30.
def filtrar_idade_superior_30(df, idade):
    return df[df['idade'] > idade]

#função que leia o arquivo transacoes e retorne um DataFrame.
def ler_arquivo_transacoes():
    df = pd.read_csv("./atividade4/transacoes.csv", sep=None, engine="python")    
    return df

#função que agrupa por categora e e soma o valor total de cada categoria.
def agrupar_categoria_soma(df):
    return df.groupby('categoria')['valor'].sum().reset_index()

#Lógica principal
df = ler_arquivo_clientes()
print("DataFrame clientes:")
print("-------------------")
print(df)
print(" ")
print("------------------------------------------")
print("DataFrame filtro idade superior a 30 anos:")
df = filtrar_idade_superior_30(df, 30) 
print(df)
df = ler_arquivo_transacoes()
print("DataFrame transacoes:")
print("---------------------")
print(df)
print(" ")
df = agrupar_categoria_soma(df)
print("---------------------------------")
print("DataFrame agrupado por categoria:")
print(df)