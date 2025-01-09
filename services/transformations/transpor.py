import polars as pl

def transpor(global_df, request):
    """Transpõe os dados do DataFrame."""
    try:
        columns_as_first_row = pl.DataFrame([global_df.columns]).rename(
            {i: col for i, col in enumerate(global_df.columns)}
        )
        updated_df = columns_as_first_row.vstack(global_df).transpose(include_header=True)
        return updated_df, None
    except Exception as e:
        return None, {"error": str(e)}
