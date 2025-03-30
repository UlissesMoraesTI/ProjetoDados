import pytest
import pandas as pd

df_json = pd.read_json('./atividade3/usuarios.json')
df_json = pd.json_normalize(df_json['usuarios'])

def test_coluna_nome_existe():
    # Verifica se o DataFrame não está vazio
    assert not df_json.empty, "O DataFrame está vazio. Verifique se o JSON foi carregado corretamente."
    
    # Verifica se a coluna 'nome' existe
    assert 'nome' in df_json.columns, "A coluna 'nome' não foi encontrada no DataFrame."

    # Verifica se a coluna 'idade' existe
    assert 'idade' in df_json.columns, "A coluna 'idade' não foi encontrada no DataFrame."

    # Verifica se o nome 'Alice' está presente na coluna 'nome'
    assert "Alice" in df_json['nome'].values, "O nome 'Alice' não foi encontrado na coluna 'nome'."

    # Verifica se o nome 'Carla' está presente na coluna 'nome'
    assert "Carla" in df_json['nome'].values, "O nome 'Carla' não foi encontrado na coluna 'nome'."

    # Verifica se Alice tem 22 Anos de Idade
    assert ((df_json['nome'] == 'Alice') & (df_json['idade'] == 22)).any(), "Não existe uma linha onde nome='Alice' e idade=22."

    # Verifica se Carla tem 25 Anos de Idade
    assert ((df_json['nome'] == 'Carla') & (df_json['idade'] == 25)).any(), "Não existe uma linha onde nome='Carla' e idade=25."