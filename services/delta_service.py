from delta.tables import DeltaTable
import os
from pyspark.sql import SparkSession
import polars as pl

# Configuração do Spark
spark = SparkSession.builder \
    .appName("DeltaLakeApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

BASE_PATH = "delta_storage"  # Diretório onde os fluxos são armazenados

def initialize_flow(flow_id):
    """Inicializa o diretório para o fluxo."""
    flow_path = os.path.join(BASE_PATH, flow_id)
    os.makedirs(flow_path, exist_ok=True)
    return flow_path

def write_to_delta(flow_id, dataframe, version_name):
    """Salva um DataFrame em Delta Lake como nova versão."""
    flow_path = initialize_flow(flow_id)
    delta_path = os.path.join(flow_path, "delta_table")
    dataframe.write.format("delta").mode("append").save(delta_path)

    return {"message": f"Versão {version_name} salva com sucesso."}

def read_from_delta(flow_id):
    """Lê os dados da última versão salva no Delta Lake."""
    delta_path = os.path.join(BASE_PATH, flow_id, "delta_table")
    if not DeltaTable.isDeltaTable(spark, delta_path):
        raise FileNotFoundError(f"Nenhuma tabela Delta encontrada para o fluxo {flow_id}")
    return spark.read.format("delta").load(delta_path)

def delete_flow(flow_id):
    """Exclui um fluxo inteiro."""
    flow_path = os.path.join(BASE_PATH, flow_id)
    if os.path.exists(flow_path):
        for root, dirs, files in os.walk(flow_path, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))
        os.rmdir(flow_path)
        return {"message": f"Fluxo {flow_id} deletado com sucesso."}
    else:
        raise FileNotFoundError(f"Fluxo {flow_id} não encontrado.")
