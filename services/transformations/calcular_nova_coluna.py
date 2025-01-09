def calcular_nova_coluna(global_df, request):
    """Calcula uma nova coluna com base em uma fórmula."""
    data = request.json
    formula = data.get("formula")
    new_column_name = data.get("new_column")

    if not formula or not new_column_name:
        return None, {"error": "Fórmula ou nome da coluna não fornecido"}

    try:
        for col in global_df.columns:
            if col in formula:
                formula = formula.replace(col, f"global_df['{col}']")
        updated_df = global_df.with_columns(eval(formula).alias(new_column_name))
        return updated_df, None
    except Exception as e:
        return None, {"error": str(e)}
