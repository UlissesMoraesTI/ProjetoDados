import pytest
import pandas as pd

# Definindo a fixture para carregar o arquivo Parquet
@pytest.fixture
def df_parquet():
    try:
        # Lendo o arquivo Parquet para um DataFrame
        return pd.read_parquet('./atividade4/dados_agregados.parquet')
    except FileNotFoundError:
        pytest.fail("Erro: O arquivo './atividade4/dados_agregados.parquet' não foi encontrado.")
    except Exception as e:
        pytest.fail(f"Erro ao tentar ler o arquivo Parquet: {e}")

# Verifica se o DataFrame não está vazio
def test_dataframe_nao_vazio(df_parquet):
    assert not df_parquet.empty, "O DataFrame está vazio. O arquivo Parquet pode estar corrompido ou sem dados."

# Verifica se a coluna "categoria" existe
def test_verificar_coluna_categoria_existe(df_parquet):
    assert 'categoria' in df_parquet.columns, "A coluna 'categoria' não foi encontrada no DataFrame."

# Verifica se a coluna "soma_valor" existe
def test_verificar_coluna_soma_valor_existe(df_parquet):
    assert 'soma_valor' in df_parquet.columns, "A coluna 'soma_valor' não foi encontrada no DataFrame."

# Verifica se a coluna "media_valor" existe
def test_verificar_coluna_media_valor_existe(df_parquet):
    assert 'media_valor' in df_parquet.columns, "A coluna 'media_valor' não foi encontrada no DataFrame."    

# Teste para verificar se o DataFrame possui 3 registros
def test_dataframe_tres_registros(df_parquet):
    assert df_parquet.shape[0] == 3, f"O DataFrame possui {df_parquet.shape[0]} registros, mas esperava-se exatamente 3."

import pytest

# Teste para verificar se a "categoria" possui o valor "A"
def test_categoria_contem_valor_A(df_parquet):
    assert 'A' in df_parquet['categoria'].values, "A categoria 'A' não foi encontrado."    

# Teste para verificar se a "categoria" possui o valor "B"
def test_categoria_contem_valor_B(df_parquet):
    assert 'B' in df_parquet['categoria'].values, "A categoria 'B' não foi encontrado."        

# Teste para verificar se a "categoria" possui o valor "C"
def test_categoria_contem_valor_C(df_parquet):
    assert 'C' in df_parquet['categoria'].values, "A categoria 'C' não foi encontrado."            

# Verifica se 'soma_valor' = 450 para a categoria "A"
def test_categoria_A_valor_soma_450(df_parquet):    
    assert df_parquet.loc[df_parquet['categoria'] == 'A', 'soma_valor'].iloc[0] == 450, "A coluna 'soma_valor' não tem o valor 450 para 'categoria' = 'A'."    

# Verifica se 'soma_valor' = 450 para a categoria "B"
def test_categoria_B_valor_soma_450(df_parquet):
    assert df_parquet.loc[df_parquet['categoria'] == 'B', 'soma_valor'].iloc[0] == 450, "A coluna 'soma_valor' não tem o valor 450 para 'categoria' = 'B'."

# Verifica se 'soma_valor' = 300 para a categoria "C"
def test_categoria_C_valor_soma_300(df_parquet):    
    assert df_parquet.loc[df_parquet['categoria'] == 'C', 'soma_valor'].iloc[0] == 300, "A coluna 'soma_valor' não tem o valor 300 para 'categoria' = 'C'."

# Verifica se 'media_valor' = 150 para a categoria "A"
def test_categoria_A_media_valor_150(df_parquet):
    assert df_parquet.loc[df_parquet['categoria'] == 'A', 'media_valor'].iloc[0] == 150.0, "O valor de 'media_valor' para 'categoria' = 'A' não é 150."

# Verifica se 'media_valor' = 225 para a categoria "B"
def test_categoria_B_media_valor_225(df_parquet):    
    assert df_parquet.loc[df_parquet['categoria'] == 'B', 'media_valor'].iloc[0] == 225.0, "O valor de 'media_valor' para 'categoria' = 'C' não é 225."

# Verifica se 'media_valor' = 300 para a categoria "C"
def test_categoria_C_media_valor_300(df_parquet):    
    assert df_parquet.loc[df_parquet['categoria'] == 'C', 'media_valor'].iloc[0] == 300.0, "O valor de 'media_valor' para 'categoria' = 'C' não é 300."