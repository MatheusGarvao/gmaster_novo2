import pandas as pd
import sqlite3
import chardet

# 1. Criar ou carregar um banco de dados com operações
def criar_bd_operacoes(nome_bd="operacoes.db"):
    conexao = sqlite3.connect(nome_bd)
    cursor = conexao.cursor()

    # Criar tabela para armazenar as operações
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Operacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome_operacao TEXT NOT NULL,
        funcao TEXT NOT NULL,
        categoria TEXT
    )
    """)

    # Inserir operações, se não estiverem presentes
    cursor.executemany("""
    INSERT OR IGNORE INTO Operacoes (nome_operacao, funcao, categoria)
    VALUES (?, ?, ?)
    """, [
        ('Soma', 'sum', 'Agregação'),
        ('Média', 'mean', 'Agregação'),
        ('Mediana', 'median', 'Agregação'),
        ('Mínimo', 'min', 'Agregação'),
        ('Máximo', 'max', 'Agregação'),
        ('Contagem', 'count', 'Agregação'),
        ('Desvio Padrão', 'std', 'Agregação'),
        ('Variância', 'var', 'Agregação'),
        ('Moda', 'mode', 'Agregação'),
        ('Primeiro Valor', 'first', 'Agregação'),
        ('Último Valor', 'last', 'Agregação'),
        ('Produto', 'prod', 'Agregação'),
        ('Texto Concatenado', 'join', 'Operação Texto'),
        ('Contagem Única', 'nunique', 'Agregação')
    ])

    # Salvar alterações e fechar conexão
    conexao.commit()
    conexao.close()


# 2. Carregar as operações do banco de dados
def carregar_operacoes(nome_bd="operacoes.db"):
    conexao = sqlite3.connect(nome_bd)
    cursor = conexao.cursor()

    # Consultar todas as operações disponíveis
    cursor.execute("SELECT nome_operacao, funcao FROM Operacoes")
    operacoes = {row[0]: row[1] for row in cursor.fetchall()}

    conexao.close()
    return operacoes


# 3. Aplicar operações ao DataFrame
def aplicar_operacoes(df, agrupamento, operacoes_escolhidas):
    """
    df: pandas.DataFrame
    agrupamento: str ou lista de colunas a serem agrupadas
    operacoes_escolhidas: dict, no formato {coluna: [operacao, ...]}
    """
    # Preparar o dicionário para o método agg()
    operacoes_para_agg = {}
    for coluna, lista_operacoes in operacoes_escolhidas.items():
        operacoes_para_agg[coluna] = lista_operacoes

    # Agrupar e aplicar as operações
    resultado = df.groupby(agrupamento).agg(operacoes_para_agg)

    # Ajustar os nomes das colunas de saída
    resultado.columns = ['_'.join(col) for col in resultado.columns]
    resultado.reset_index(inplace=True)

    return resultado


# 4. Carregar base de dados (CSV ou XLSX)
def carregar_base_dados(arquivo):
    """
    arquivo: str, caminho para o arquivo CSV ou XLSX
    """
    if arquivo.endswith('.csv'):
        # Detectar a codificação
        with open(arquivo, 'rb') as f:
            raw_data = f.read()
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        print(f"Codificação detectada: {encoding}")
        
        # Tentar carregar com delimitador `;` e ajustar, se necessário
        try:
            return pd.read_csv(arquivo, encoding=encoding, sep=';')
        except Exception as e:
            print("Erro ao carregar CSV:", e)
            raise
    elif arquivo.endswith('.xlsx'):
        return pd.read_excel(arquivo)
    else:
        raise ValueError("Formato de arquivo não suportado. Use .csv ou .xlsx.")


# 5. Exportar resultado para arquivo
def exportar_resultado(df, caminho_saida):
    """
    df: pandas.DataFrame
    caminho_saida: str, caminho do arquivo de saída (.csv ou .xlsx)
    """
    if caminho_saida.endswith('.csv'):
        df.to_csv(caminho_saida, index=False)
    elif caminho_saida.endswith('.xlsx'):
        df.to_excel(caminho_saida, index=False)
    else:
        raise ValueError("Formato de arquivo não suportado. Use .csv ou .xlsx.")


# 6. Exemplo de uso
if __name__ == "__main__":
    # Criar banco de dados e carregar operações
    criar_bd_operacoes()
    operacoes_disponiveis = carregar_operacoes()
    print("Operações disponíveis:")
    for nome_operacao in operacoes_disponiveis:
        print(f"- {nome_operacao}")

    # Carregar base de dados
    arquivo = input("Digite o caminho do arquivo (CSV ou XLSX): ")
    df = carregar_base_dados(arquivo)
    print("Colunas disponíveis no DataFrame:", df.columns.tolist())
    print("\nDados carregados:")
    print(df.head())

    # Escolher coluna para agrupamento
    agrupamento = input("Digite o nome da coluna para agrupamento: ")

    # Escolher operações para aplicar
    operacoes_escolhidas = {}
    while True:
        coluna = input("Digite o nome da coluna para aplicar as operações (ou 'sair' para terminar): ")
        if coluna == 'sair':
            break
        if coluna not in df.columns:
            print(f"A coluna '{coluna}' não existe no DataFrame.")
            continue
        
        operacoes = []
        print(f"Operações disponíveis para a coluna '{coluna}':")
        for nome_operacao in operacoes_disponiveis:
            print(f"- {nome_operacao}")
        
        while True:
            operacao = input(f"Escolha uma operação para a coluna '{coluna}' (ou 'sair' para terminar): ")
            if operacao == 'sair':
                break
            if operacao not in operacoes_disponiveis:
                print(f"A operação '{operacao}' não é válida.")
                continue
            operacoes.append(operacoes_disponiveis[operacao])
        
        if operacoes:
            operacoes_escolhidas[coluna] = operacoes

    # Aplicar operações
    resultado = aplicar_operacoes(df, agrupamento, operacoes_escolhidas)

    # Exibir resultado
    print("\nResultado final:")
    print(resultado)

    # Exportar resultado
    caminho_saida = input("Digite o caminho para salvar o resultado (CSV ou XLSX): ")
    exportar_resultado(resultado, caminho_saida)
    print(f"Resultado salvo em: {caminho_saida}")
