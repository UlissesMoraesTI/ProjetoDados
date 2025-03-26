import pandas as pd

#função que leia o arquivo alunos.csv e retorne um DataFrame.
def ler_arquivo_alunos():
    df_csv = pd.read_csv('./atividade2/alunos.csv', delimiter=';')
    return df_csv

#função que calcule a média das notas para cada aluno.
def calcular_media_aluno(df):
    df['media'] = df[['nota1', 'nota2', 'nota3']].mean(axis=1)
    return df

#função que identifica alunos com média maior ou igual a 7.
def identifica_media_aluno(df):
    df['resultado'] = df['media'].apply(lambda x: 'Aprovado(a)' if x >= 7 else 'Reprovado(a)')
    return df 

#função seleciona colunas[nome, resultado]
def seleciona_colunas(df):
    return df[['aluno', 'resultado']]

#função DataFrame Saída em arquivo CSV.
def salvar_media_alunos(df):    
    df.to_csv('./atividade2/media_alunos.csv', sep=';', index=False)    

#Lógica principal
df = ler_arquivo_alunos()
df = calcular_media_aluno(df)
df = identifica_media_aluno(df)
df = seleciona_colunas(df)
salvar_media_alunos(df) 

#imprimir o DataFrame
print("Alunos")
print(df)
print("Fim do programa!")     