import pytest
import pandas as pd

# Função a ser testada
def ler_arquivo_media_alunos():
    df_csv = pd.read_csv('./atividade2/media_alunos.csv', delimiter=';')
    return df_csv

# Verificar se a coluna 'aluno' está presente no DataFrame
def test_coluna_aluno_existe():
    df = ler_arquivo_media_alunos()        
    assert 'aluno' in df.columns, "A coluna 'aluno' não foi encontrada no DataFrame"

# Verificar se a coluna 'resultado' está presente no DataFrame
def test_coluna_resultado_existe():
    df = ler_arquivo_media_alunos()        
    assert 'resultado' in df.columns, "A coluna 'resultado' não foi encontrada no DataFrame"    

# Verificar a quantidade de linhas presentes no DataFrame
def test_quantidade_linhas_dataframe():
    df = ler_arquivo_media_alunos()    
    assert len(df) == 4, f"A quantidade de linhas do DataFrame é {len(df)}"

# Verificar se existe o aluno "Bruno"
def test_existe_aluno_Bruno():
    df = ler_arquivo_media_alunos()    
    assert 'Bruno' in df['aluno'].values, "O aluno 'Bruno' não foi encontrado no DataFrame"    

# Verificar se existe o aluno "Carlos"
def test_existe_aluno_Carlos():
    df = ler_arquivo_media_alunos()    
    assert 'Carlos' in df['aluno'].values, "O aluno 'Carlos' não foi encontrado no DataFrame"        

# Filtrando a linha com "Aluno Bruno" - Verificar resultado
def test_resultado_aluno_Bruno():
    df = ler_arquivo_media_alunos()    
    resultado_aluno_Bruno = df[df['aluno'] == 'Bruno']['resultado'].values[0]
    assert resultado_aluno_Bruno == 'Reprovado(a)', f"O aluno 'Bruno' foi {resulado_aluno_Bruno}"

# Filtrando a linha com "Aluno Carlos" - Verificar resultado
def test_resultado_aluno_Carlos():
    df = ler_arquivo_media_alunos()    
    resultado_aluno_Carlos = df[df['aluno'] == 'Carlos']['resultado'].values[0]
    assert resultado_aluno_Carlos == 'Aprovado(a)', f"O aluno 'Carlos' foi {resultado_aluno_Carlos}"