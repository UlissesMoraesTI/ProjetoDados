from atividade1.atividade1 import atividade1
  
#teste que verifica se a função ler_arquivo_vendas() está lendo o arquivo vendas.csv corretamente.
def test_ler_arquivo_vendas():
    from atividade1 import ler_arquivo_vendas
    df = ler_arquivo_vendas()
    assert df is not None