import os
import polars as pl
from datetime import datetime
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

BASE_PATH = "delta_storage"

builder = SparkSession.builder \
    .appName("DeltaLakeApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

def initialize_flow(flow_id):
    """
    Inicializa (cria) a pasta principal do fluxo caso não exista.
    Por convenção, chamaremos essa pasta de 'main'.
    """
    flow_path = os.path.join(BASE_PATH, flow_id)
    os.makedirs(flow_path, exist_ok=True)

    main_path = os.path.join(flow_path, "main")
    os.makedirs(main_path, exist_ok=True)
    return flow_path


def _get_branch_path(flow_id, branch_name):
    """
    Retorna o path absoluto de uma branch específica.
    Exemplo: delta_storage/<flow_id>/<branch_name>
    """
    flow_path = os.path.join(BASE_PATH, flow_id)
    return os.path.join(flow_path, branch_name)


def _branch_exists(flow_id, branch_name):
    """
    Verifica se uma determinada branch (pasta) existe no flow.
    """
    branch_path = _get_branch_path(flow_id, branch_name)
    return os.path.isdir(branch_path) and os.path.exists(branch_path)


def write_to_delta(flow_id, dataframe, version_name="default", base_branch=None):
    """
    Salva um DataFrame em Delta Lake como nova versão, com controle de branches.

    - flow_id: Identificador do fluxo
    - dataframe: DataFrame do Spark a ser salvo
    - version_name: nome ou tag para a "versão" (ex: "v1", "replace_value" etc.)
    - base_branch: se None, usa "main". Se for algo existente, bifurca a partir daquele branch.
    
    Regras:
    1) Se base_branch é None => gravar em 'main' (append)
    2) Se base_branch existe, cria uma NOVA pasta => <base_branch>_<version_name> (ou outro critério)
       e grava lá. Assim gera a bifurcação.
    Retorna o caminho relativo criado (branch_name).
    """
    initialize_flow(flow_id)  

    if base_branch is None:
        branch_name = "main"
        branch_path = _get_branch_path(flow_id, branch_name)
        delta_path = os.path.join(branch_path, "delta_table")
        dataframe.write.format("delta").mode("append").save(delta_path)
        return {
            "message": f"Versão {version_name} salva com sucesso em branch '{branch_name}' (append).",
            "branch": branch_name
        }
    else:
        if not _branch_exists(flow_id, base_branch):
            raise FileNotFoundError(
                f"Branch '{base_branch}' não existe em flow_id={flow_id}."
            )
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_branch_name = f"{base_branch}_{version_name}_{timestamp}"
        
        new_branch_path = _get_branch_path(flow_id, new_branch_name)
        os.makedirs(new_branch_path, exist_ok=True)


        delta_path = os.path.join(new_branch_path, "delta_table")
        dataframe.write.format("delta").mode("append").save(delta_path)
        
        return {
            "message": f"Versão {version_name} salva com sucesso em nova branch '{new_branch_name}'.",
            "branch": new_branch_name
        }


def read_from_delta(flow_id, branch=None):
    """
    Lê dados do Delta Lake.
    - Se branch for None, retorna UM dicionário contendo:
        { "branches": [ lista de branches disponíveis ],
          "latest_data": {branchX: <top 5 records>, branchY: <top 5 records>, ...}
        }
      (ou algo do tipo)
    - Se branch for um string específico, lê somente aquela branch.
    """
    flow_path = os.path.join(BASE_PATH, flow_id)
    if not os.path.exists(flow_path):
        raise FileNotFoundError(f"Flow {flow_id} não encontrado em {BASE_PATH}.")

    branches = [
        d for d in os.listdir(flow_path)
        if os.path.isdir(os.path.join(flow_path, d))
    ]
    if not branches:
        raise FileNotFoundError(f"Nenhuma branch encontrada para o flow {flow_id}.")

    if branch is not None:
        if branch not in branches:
            raise FileNotFoundError(f"Branch '{branch}' não encontrada em flow {flow_id}.")
        branch_path = _get_branch_path(flow_id, branch)
        delta_path = os.path.join(branch_path, "delta_table")
        if not DeltaTable.isDeltaTable(spark, delta_path):
            raise FileNotFoundError(f"Nenhuma tabela Delta encontrada em {delta_path}")

        df = spark.read.format("delta").load(delta_path)
        return df
    
    result = {}
    for b in branches:
        delta_path = os.path.join(flow_path, b, "delta_table")
        if DeltaTable.isDeltaTable(spark, delta_path):
            df = spark.read.format("delta").load(delta_path)
            data_dict = df.limit(5).toPandas().to_dict()
            result[b] = data_dict
        else:
            result[b] = None 
    return {
        "branches": branches,
        "latest_data": result
    }


def delete_flow(flow_id):
    """Exclui um fluxo inteiro (todas as branches)."""
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
