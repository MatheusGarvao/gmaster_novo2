import polars as pl

def load_dataframe(data):
    """Carrega os dados em um DataFrame Polars."""
    return pl.DataFrame(data)
