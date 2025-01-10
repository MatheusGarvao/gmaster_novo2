from flask import Flask, request, jsonify
from services.database_service import configure_connection, load_data
from services.upload_service import handle_upload
from services.file_manager import delete_uploaded_file
from services.delta_service import (
    spark,        
    write_to_delta,
    read_from_delta,
    delete_flow
)

from services.transformations.replace_value import replace_value
from services.transformations.transpor import transpor
from services.transformations.rename_column import rename_column
from services.transformations.calcular_nova_coluna import calcular_nova_coluna
from services.transformations.sumarizar import sumarizar
from services.transformations.calcular_media_ponderada import calcular_media_ponderada

app = Flask(__name__)


@app.route('/upload/<int:project_id>', methods=['POST'])
def upload_route(project_id):
    if 'file' not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado."}), 400
    
    file = request.files['file']
    response, status = handle_upload(file, project_id=project_id)
    return jsonify(response), status



@app.route('/file/delete', methods=['DELETE'])
def delete_uploaded_file_route():
    """Exclui um arquivo base enviado."""
    data = request.json
    if not data or "filename" not in data:
        return jsonify({"error": "Nome do arquivo não fornecido."}), 400
    try:
        response = delete_uploaded_file(data["filename"])
        return jsonify(response), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/flow/<flow_id>/write', methods=['POST'])
def write_flow(flow_id):
    """Salva dados no fluxo Delta, possivelmente em uma branch."""
    try:
        data = request.json
        df = spark.createDataFrame(data["data"])
        
        version_name = data.get("version_name", "default")
        base_branch = data.get("base_branch")  

        response = write_to_delta(
            flow_id=flow_id,
            dataframe=df,
            version_name=version_name,
            base_branch=base_branch
        )
        
        return jsonify(response), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route('/flow/<flow_id>/read', methods=['GET'])
def read_flow(flow_id):
    """Lê dados do fluxo Delta, possivelmente de uma branch específica."""
    try:
        branch = request.args.get("branch")  # se não tiver, vem None
        result = read_from_delta(flow_id, branch=branch)
        
        # Se a função read_from_delta retornar um DF Spark quando branch != None
        # Precisamos converter pra exibir. Se for um dict (quando branch==None),
        # podemos retornar diretamente.
        
        if branch:
            # A função read_from_delta(flow_id, branch=branch) retorna um DF Spark
            df = result
            data_dict = df.limit(5).toPandas().to_dict()
            return jsonify({"data": data_dict}), 200
        else:
            return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route('/flow/<flow_id>/delete', methods=['DELETE'])
def delete_flow_route(flow_id):
    """Exclui um fluxo Delta."""
    try:
        response = delete_flow(flow_id)
        return jsonify(response), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/flow/<flow_id>/replace_value', methods=['POST'])
def replace_value_route(flow_id):
    """Aplica substituição de valores no fluxo Delta."""
    try:
        df = read_from_delta(flow_id)
        updated_df, error = replace_value(df, request)
        if error:
            return jsonify(error), 400
        write_to_delta(flow_id, updated_df, "replace_value")
        return jsonify({"message": "Valor substituído com sucesso."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/flow/<flow_id>/transpor', methods=['POST'])
def transpor_route(flow_id):
    """Transpõe os dados no fluxo Delta."""
    try:
        df = read_from_delta(flow_id)
        updated_df, error = transpor(df, request)
        if error:
            return jsonify(error), 400
        write_to_delta(flow_id, updated_df, "transpor")
        return jsonify({"message": "Dados transpostos com sucesso."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/flow/<flow_id>/rename_column', methods=['POST'])
def rename_column_route(flow_id):
    """Renomeia colunas no fluxo Delta."""
    try:
        df = read_from_delta(flow_id)
        updated_df, error = rename_column(df, request)
        if error:
            return jsonify(error), 400
        write_to_delta(flow_id, updated_df, "rename_column")
        return jsonify({"message": "Coluna renomeada com sucesso."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/flow/<flow_id>/calcular_nova_coluna', methods=['POST'])
def calcular_nova_coluna_route(flow_id):
    """Calcula uma nova coluna no fluxo Delta."""
    try:
        df = read_from_delta(flow_id)
        updated_df, error = calcular_nova_coluna(df, request)
        if error:
            return jsonify(error), 400
        write_to_delta(flow_id, updated_df, "calcular_nova_coluna")
        return jsonify({"message": "Nova coluna calculada com sucesso."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/flow/<flow_id>/sumarizar', methods=['POST'])
def sumarizar_route(flow_id):
    """Sumariza os dados no fluxo Delta."""
    try:
        df = read_from_delta(flow_id)
        updated_df, error = sumarizar(df, request)
        if error:
            return jsonify(error), 400
        write_to_delta(flow_id, updated_df, "sumarizar")
        return jsonify({"message": "Dados sumarizados com sucesso."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/flow/<flow_id>/calcular_media_ponderada', methods=['POST'])
def calcular_media_ponderada_route(flow_id):
    """Calcula a média ponderada no fluxo Delta."""
    try:
        df = read_from_delta(flow_id)
        updated_df, error = calcular_media_ponderada(df, request)
        if error:
            return jsonify(error), 400
        write_to_delta(flow_id, updated_df, "calcular_media_ponderada")
        return jsonify({"message": "Média ponderada calculada com sucesso."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/flow/<flow_id>/load_from_db', methods=['POST'])
def load_from_db_route(flow_id):
    """
    Carrega dados do banco via:
      - Uma query SQL arbitrária (field 'query'), ou
      - O nome de uma tabela (field 'table_name')
    e salva no fluxo Delta.
    """
    try:
        data = request.json
        db_type = data.get("db_type") 
        env_file = data.get("env_file", ".env")  
        table_name = data.get("table_name")
        query = data.get("query")
        version_name = data.get("version_name", "from_db")

        if not db_type:
            return jsonify({"error": "db_type é obrigatório"}), 400
        if not table_name and not query:
            return jsonify({"error": "Necessário fornecer 'table_name' ou 'query'"}), 400

        engine = configure_connection(db_type, env_file)

        df_polars = load_data(engine, table_name=table_name, query=query)

        df_spark = spark.createDataFrame(df_polars.to_pandas())

        response = write_to_delta(flow_id, df_spark, version_name=version_name)

        return jsonify(response), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500




if __name__ == '__main__':
    app.run(debug=True)

