import os
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import polars as pl
import numpy as np
import logging

class DatabaseConnectionManager:
    def __init__(self):
        self.supported_dbs = {
            'postgres': {
                'env_file': 'postgres_config.env',
                'connection_string': "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
            },
            'mysql': {
                'env_file': 'mysql_config.env',
                'connection_string': "mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
            },
            'sqlite': {
                'env_file': 'sqlite_config.env',
                'connection_string': "sqlite:///{database}"
            },
            'mssql': {
                'env_file': 'mssql_config.env',
                'connection_string': "mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
            }
        }
        self.current_db_type = None
        self.engine = None

    def load_db_config(self, db_type: str):
        if db_type not in self.supported_dbs:
            raise ValueError(f"Tipo de banco de dados não suportado. Opções válidas: {list(self.supported_dbs.keys())}")
            
        db_config = self.supported_dbs[db_type]
        env_path = Path(__file__).resolve().parent / db_config['env_file']
        if not env_path.exists():
            raise FileNotFoundError(f"Arquivo de configuração {env_path} não encontrado.")
        
        load_dotenv(dotenv_path=env_path, verbose=True)
        config = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': os.getenv('DB_NAME')
        }
        
        return config, db_config['connection_string']
    
    def configure_connection(self, db_type: str):
        """Configura a conexão com o banco de dados."""
        try:
            config, connection_string = self.load_db_config(db_type)
            if db_type == 'sqlite':
                self.engine = create_engine(connection_string.format(database=config['database']))
            else:
                self.engine = create_engine(connection_string.format(**config))
            self.current_db_type = db_type
            logging.info(f"Conexão configurada com sucesso para o banco de dados: {db_type}")
        except Exception as e:
            logging.error(f"Erro ao configurar conexão: {e}")
            raise
    
    def load_table_data(self, table_name: str):
        """Carrega os dados de uma tabela do banco configurado."""
        if not self.engine:
            raise ValueError("Conexão com o banco de dados não configurada.")
        if not table_name:
            raise ValueError("Nome da tabela não fornecido.")
        
        # Lê a tabela usando SQLAlchemy e converte para polars
        query = text(f"SELECT * FROM {table_name}")  # Use text para tornar a consulta executável
        with self.engine.connect() as connection:
            result = connection.execute(query)
            columns = result.keys()
            rows = result.fetchall()
            df = pl.DataFrame(rows, schema=columns)

        print("Esquema do DataFrame:", df.schema)  # Depuração do esquema
        print(df.head())
                
        # Tratar colunas datetime (convertendo para string)
        for col in df.select(pl.col(pl.Datetime)).columns:
            df = df.with_columns(pl.col(col).dt.strftime("%Y-%m-%d %H:%M:%S").alias(col))

        # Retornar os dados como lista de dicionários
        return df.fill_null("N/A").to_dicts()
