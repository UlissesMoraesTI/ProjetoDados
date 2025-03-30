import pytest
import pandas as pd

# Função a ser testada
def ler_arquivo_vendas():
    df_csv = pd.read_csv('./atividade1/vendas_filtradas.csv', delimiter=';')
    return df_csv

# Verificar se a coluna 'produto' está presente no DataFrame
def test_coluna_produto_existe():
    df = ler_arquivo_vendas()        
    assert 'produto' in df.columns, "A coluna 'produto' não foi encontrada no DataFrame"

# Verificar se a coluna 'quantidade' está presente no DataFrame
def test_coluna_quantidade_existe():
    df = ler_arquivo_vendas()    
    assert 'quantidade' in df.columns, "A coluna 'quantidade' não foi encontrada no DataFrame"

# Verificar se a coluna 'preco_unitario' está presente no DataFrame
def test_coluna_preco_unitario_existe():
    df = ler_arquivo_vendas()    
    assert 'preco_unitario' in df.columns, "A coluna 'preco_unitario' não foi encontrada no DataFrame"

# Verificar se a coluna 'valor_total' está presente no DataFrame
def test_coluna_valor_total_existe():
    df = ler_arquivo_vendas()    
    assert 'valor_total' in df.columns, "A coluna 'valor_total' não foi encontrada no DataFrame"

# Verificar a quantidade de linhas presentes no DataFrame
def test_quantidade_linhas_dataframe():
    df = ler_arquivo_vendas()    
    assert len(df) == 2, f"A quantidade de linhas do DataFrame é {len(df)}"

# Verificar se existe o produto "Produto B"
def test_existe_produto_B():
    df = ler_arquivo_vendas()    
    assert 'Produto B' in df['produto'].values, "O produto 'Produto B' não foi encontrado no DataFrame"    

# Verificar se existe o produto "Produto C"
def test_existe_produto_C():
    df = ler_arquivo_vendas()    
    assert 'Produto C' in df['produto'].values, "O produto 'Produto C' não foi encontrado no DataFrame"    

# Filtrando a linha com "Produto B" - Verificar quantidade
def test_quantidade_produto_B():
    df = ler_arquivo_vendas()    
    quantidade_produto_B = df[df['produto'] == 'Produto B']['quantidade'].values[0]
    assert quantidade_produto_B == 5, f"A quantidade do produto 'Produto B' é {quantidade_produto_B}"

# Filtrando a linha com "Produto B" - Verificar preco_unitario
def test_quantidade_produto_B():
    df = ler_arquivo_vendas()    
    preco_unitario_produto_B = df[df['produto'] == 'Produto B']['preco_unitario'].values[0]
    assert preco_unitario_produto_B == 150.0, f"O preco unitario do produto 'Produto B' é {preco_unitario_produto_B}"    

# Filtrando a linha com "Produto B" - Verificar valor_total
def test_valor_total_B():
    df = ler_arquivo_vendas()    
    valor_total_produto_B = df[df['produto'] == 'Produto B']['valor_total'].values[0]
    assert valor_total_produto_B == 750.0, f"O valor total do produto 'Produto B' é {valor_total_produto_B}"       