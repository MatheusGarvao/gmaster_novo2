import polars as pl

def calcular_media_ponderada(global_df, request):
    """Calcula a média ponderada."""
    data = request.json
    valor_col = data.get("valor_col")
    peso_col = data.get("peso_col")
    output_col = data.get("output_col", "Media_Ponderada")

    if not valor_col or not peso_col:
        return None, {"error": "Colunas valor_col e peso_col são obrigatórias"}

    try:
        soma_ponderada = (global_df[valor_col] * global_df[peso_col]).sum()
        soma_pesos = global_df[peso_col].sum()

        if soma_pesos == 0:
            return None, {"error": "A soma dos pesos é zero"}

        media_ponderada = soma_ponderada / soma_pesos
        updated_df = global_df.with_columns(pl.lit(media_ponderada).alias(output_col))
        return updated_df, None
    except Exception as e:
        return None, {"error": str(e)}
