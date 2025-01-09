def rename_column(global_df, request):
    """Renomeia uma coluna."""
    data = request.json
    current_column = data.get("currentColumn")
    new_column_name = data.get("newColumnName")

    if not current_column or not new_column_name:
        return None, {"error": "Parâmetros incompletos"}

    try:
        updated_df = global_df.rename({current_column: new_column_name})
        return updated_df, None
    except Exception as e:
        return None, {"error": str(e)}
