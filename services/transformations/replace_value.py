import polars as pl

def replace_value(global_df, request):
    """Substitui um valor por outro em uma coluna específica."""
    data = request.json
    column = data.get("column")
    old_value = data.get("oldValue")
    new_value = data.get("newValue")

    if not column or old_value is None or new_value is None:
        return None, {"error": "Parâmetros incompletos"}

    try:
        updated_df = global_df.with_columns(
            pl.when(pl.col(column) == float(old_value))
            .then(float(new_value))
            .otherwise(pl.col(column))
            .alias(column)
        )
        return updated_df, None
    except Exception as e:
        return None, {"error": str(e)}
