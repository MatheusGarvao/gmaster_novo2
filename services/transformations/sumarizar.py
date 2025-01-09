import polars as pl

def sumarizar(global_df, request):
    """Aplica operações de agregação em um DataFrame."""
    data = request.json
    agrupamento = data.get('agrupamento', [])
    operacoes = data.get('operacoes', {})

    if not agrupamento:
        return None, {"error": "Coluna de agrupamento não fornecida"}

    try:
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

        agregacoes = []
        for coluna, operacoes_coluna in operacoes.items():
            for operacao in operacoes_coluna:
                if operacao not in mapeamento_operacoes:
                    return None, {"error": f"Operação {operacao} não suportada"}
                agregacoes.append(
                    mapeamento_operacoes[operacao](coluna).alias(f"{coluna}_{operacao}")
                )

        updated_df = global_df.group_by(agrupamento).agg(agregacoes)
        return updated_df, None
    except Exception as e:
        return None, {"error": str(e)}
