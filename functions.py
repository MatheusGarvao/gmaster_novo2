import polars as pl
from flask import jsonify

# Função global para carregar o DataFrame
def load_dataframe(data):
    """Carrega os dados em um DataFrame Polars."""
    return pl.DataFrame(data)

def replace_value(global_df, request):
    """Substitui um valor por outro em uma coluna específica."""
    data = request.json
    column = data.get("column")
    old_value = data.get("oldValue")
    new_value = data.get("newValue")

    if not column or old_value is None or new_value is None:
        return None, {"error": "Parâmetros incompletos"}

    try:
        # Converte os valores para numéricos, se possível
        old_value = float(old_value)
        new_value = float(new_value)

        # Substitui o valor na coluna especificada e retorna o DataFrame atualizado
        updated_df = global_df.with_columns(
            pl.when(pl.col(column) == old_value)
            .then(new_value)
            .otherwise(pl.col(column))
            .alias(column)
        )
        return updated_df, None
    except Exception as e:
        return None, {"error": str(e)}


def transpor(global_df, request):
    """Transpõe os dados do DataFrame."""
    data = request.json

    if "data" not in data:
        return jsonify({"error": "Dados não fornecidos."}), 400

    try:
        # Cria um DataFrame Polars a partir dos dados recebidos
        df = pl.DataFrame(data["data"])

        # Adiciona os nomes das colunas como a primeira linha
        columns_as_first_row = pl.DataFrame([df.columns]).rename(
            {i: col for i, col in enumerate(df.columns)}
        )
        global_df = columns_as_first_row.vstack(df).transpose(include_header=True)

        return jsonify(global_df.to_dicts())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def rename_column(global_df, request):
    """Renomeia uma coluna."""
    data = request.json
    current_column = data.get("currentColumn")
    new_column_name = data.get("newColumnName")

    if not current_column or not new_column_name:
        return jsonify({"error": "Nome atual e novo nome são necessários."}), 400

    try:
        # Renomeia a coluna no DataFrame Polars
        updated_df = global_df.rename({current_column: new_column_name})
        return updated_df, None
    except Exception as e:
        return None,{"error": str(e)}, 500


def calcular_nova_coluna(global_df, request):
    """Calcula uma nova coluna com base em uma fórmula."""
    data = request.json
    formula = data.get("formula")
    new_column_name = data.get("new_column")

    if not formula:
        return jsonify({"error": "Fórmula não fornecida"}), 400
    if not new_column_name:
        new_column_name = f"{formula} (Nova)"

    try:
        # Adapta os nomes das colunas na fórmula para serem compatíveis com Polars
        for col in global_df.columns:
            if col in formula:
                formula = formula.replace(col, f"global_df['{col}']")

        # Avalia a fórmula e adiciona uma nova coluna
        updated_df = global_df.with_columns(eval(formula).alias(new_column_name))
        return updated_df, None
    except Exception as e:
        return None,{"error": str(e)}, 500
    
def sumarizar(global_df, request):
    """
    Função para aplicar operações de agregação via API com Polars
    """
    try:
        # Receber dados da requisição
        data = request.json
        
        # Extrair parâmetros
        agrupamento = data.get('agrupamento', [])
        operacoes_escolhidas = data.get('operacoes', {})
        
        # Validar entrada
        if not agrupamento:
            return jsonify({"error": "Coluna de agrupamento não fornecida"}), 400
        if not isinstance(agrupamento, list):
            agrupamento = [agrupamento]

        # Validar colunas de agrupamento
        for col in agrupamento:
            if col not in global_df.columns:
                return jsonify({"error": f"Coluna de agrupamento '{col}' não encontrada"}), 400

        # Validar colunas para operações
        for coluna in operacoes_escolhidas.keys():
            if coluna not in global_df.columns:
                return jsonify({"error": f"Coluna '{coluna}' para operações não encontrada"}), 400
        
        # Mapear operações
        mapeamento_operacoes = {
            'soma': lambda col: pl.col(col).sum(),
            'media': lambda col: pl.col(col).mean(),
            'mediana': lambda col: pl.col(col).median(),
            'minimo': lambda col: pl.col(col).min(),
            'maximo': lambda col: pl.col(col).max(),
            'contagem': lambda col: pl.col(col).count(),
            'desvio_padrao': lambda col: pl.col(col).std(),
            'primeiro': lambda col: pl.col(col).first(),
            'ultimo': lambda col: pl.col(col).last()
        }
        # Preparar as agregações
        agregacoes = []
        for coluna, lista_operacoes in operacoes_escolhidas.items():
            for operacao in lista_operacoes:
                if operacao not in mapeamento_operacoes:
                    return jsonify({"error": f"Operação {operacao} não suportada"}), 400
                agregacoes.append(
                    mapeamento_operacoes[operacao](coluna).alias(f"{coluna}_{operacao}")
                )
        
        # Realizar o agrupamento e agregação
        updated_df = global_df.group_by(agrupamento).agg(agregacoes)
        print(updated_df.head())
        
        # Converter para dicionário para resposta JSON
        return updated_df, None
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
def calcular_media_ponderada(global_df, request):
    """
    Calcula a média ponderada de um campo numérico com base em um campo de pesos.
    """
    data = request.json
    valor_col = data.get("valor_col")  # Campo de valores
    peso_col = data.get("peso_col")    # Campo de pesos
    output_col = data.get("output_col", "Media_Ponderada")  # Nome do campo de saída

    # Verificar se os campos necessários foram fornecidos
    if not valor_col or not peso_col:
        return jsonify({"error": "Campos 'valor_col' e 'peso_col' são obrigatórios."}), 400

    try:
        # Verificar se as colunas existem no DataFrame
        if valor_col not in global_df.columns or peso_col not in global_df.columns:
            return jsonify({"error": f"Uma ou mais colunas fornecidas não existem no DataFrame: {valor_col}, {peso_col}"}), 400

        # Calcular a soma ponderada e a soma dos pesos
        soma_ponderada = (global_df[valor_col] * global_df[peso_col]).sum()
        soma_pesos = global_df[peso_col].sum()

        if soma_pesos == 0:
            return jsonify({"error": "A soma dos pesos é zero. Não é possível calcular a média ponderada."}), 400

        # Calcular a média ponderada
        media_ponderada = soma_ponderada / soma_pesos

        # Adicionar o resultado ao DataFrame como uma nova coluna
        updated_df = global_df.with_columns(pl.lit(media_ponderada).alias(output_col))

        return updated_df, None
    except Exception as e:
        return jsonify({"error": str(e)}), 500
