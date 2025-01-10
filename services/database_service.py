from sqlalchemy import create_engine, text
import polars as pl
import os
from dotenv import load_dotenv
from pathlib import Path

def configure_connection(db_type, env_file):
    """Configura a conexão com o banco de dados."""
    supported_dbs = {
        'postgres': "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}",
        'mysql': "mysql+pymysql://{user}:{password}@{host}:{port}/{database}",
        'sqlite': "sqlite:///{database}",
        'mssql': "mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    }

    if db_type not in supported_dbs:
        raise ValueError(f"Tipo de banco de dados '{db_type}' não suportado.")

    env_path = Path(env_file)
    if not env_path.exists():
        raise FileNotFoundError(f"Arquivo de configuração {env_file} não encontrado.")

    load_dotenv(dotenv_path=env_path)

    connection_string = supported_dbs[db_type].format(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME")
    )

    engine = create_engine(connection_string)
    return engine


def load_data(engine, table_name=None, query=None):
    """
    Carrega dados de um banco e retorna um DataFrame Polars.
    Se 'query' for fornecida, usa a query. 
    Senão, se 'table_name' for fornecida, faz SELECT * dessa tabela.
    """
    if query:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            columns = result.keys()
            rows = result.fetchall()
            df = pl.DataFrame(rows, schema=columns)
        return df.fill_null("N/A")

    elif table_name:
        sql = f"SELECT * FROM {table_name}"
        with engine.connect() as connection:
            result = connection.execute(text(sql))
            columns = result.keys()
            rows = result.fetchall()
            df = pl.DataFrame(rows, schema=columns)
        return df.fill_null("N/A")

    else:
        raise ValueError("É necessário fornecer 'query' ou 'table_name'.")

