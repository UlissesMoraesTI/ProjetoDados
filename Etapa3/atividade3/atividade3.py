import pandas as pd
import json

#função que leia o json usuarios.json e retorne um DataFrame.
def ler_json_usuarios():
    df_json = pd.read_json('./atividade3/usuarios.json')
    return df_json

#função que converte o json para um DataFrame
def converte_json_to_df(df_json):
    df_json = pd.json_normalize(df_json['usuarios'])
    return df_json

#função filtro os usuários com idade maior que 18 anos.
def filtrar_usuarios_idade_maior_que_18(df_json, idade):
    return df_json[df_json['idade'] > idade]

#função que ordene os usuários por idade
def ordenar_usuarios_por_idade(df_json):
    return df_json.sort_values(by='idade')

#função gerar relatório formato lista dicionários
def gerar_relatorio_lista_dicionarios(df_json):
    return df_json.to_dict('records')

#função ajuste saída json para aspas duplas
def ajustar_saida_json(df_json):
    return json.dumps(df_json, ensure_ascii=False, indent=4)

#Lógica principal
df = ler_json_usuarios()
df = converte_json_to_df(df)
df = filtrar_usuarios_idade_maior_que_18(df, 18)
df = ordenar_usuarios_por_idade(df)
df = gerar_relatorio_lista_dicionarios(df)
df = ajustar_saida_json(df)
 
#imprimir o DataFrame
print("Usuarios")
print(df)
print("Fim do programa!")     