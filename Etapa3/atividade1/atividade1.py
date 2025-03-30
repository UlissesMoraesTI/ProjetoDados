import pandas as pd

#função que leia o arquivo vendas.csv e retorne um DataFrame.
def ler_arquivo_vendas():
    df_csv = pd.read_csv('./atividade1/vendas.csv', delimiter=';')
    return df_csv

#função nova coluna valor_total(quantidade * preco_unitario)
def calcular_valor_total(df):
    df['valor_total'] = df['quantidade'] * df['preco_unitario']
    return df

#função filtro valor_total superior a R$500.
def filtrar_valor_total_superior(df, valor):
    return df[df['valor_total'] > valor]

#função DataFrame Saída em arquivo CSV.
def salvar_arquivo_vendas(df):
    df.to_csv('./atividade1/vendas_filtradas.csv', sep=';', index=False)

#Lógica principal
df = ler_arquivo_vendas()
df = calcular_valor_total(df)
df = filtrar_valor_total_superior(df, 500) 
salvar_arquivo_vendas(df)

#imprimir o DataFrame
print("Valor com valor total > 500:")
print(df)
print("Fim do programa!")     