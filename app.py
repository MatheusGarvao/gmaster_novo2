from flask import Flask, request, jsonify
from services.upload_service import handle_upload
from services.file_manager import delete_uploaded_file
from services.delta_service import write_to_delta, read_from_delta, delete_flow
from services.transformations.replace_value import replace_value
from services.transformations.transpor import transpor
from services.transformations.rename_column import rename_column
from services.transformations.calcular_nova_coluna import calcular_nova_coluna
from services.transformations.sumarizar import sumarizar
from services.transformations.calcular_media_ponderada import calcular_media_ponderada
from pyspark.sql import SparkSession

# Configuração do Flask
app = Flask(__name__)

# Configuração do Spark para Delta Lake
spark = SparkSession.builder \
    .appName("DeltaLakeApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

@app.route('/upload', methods=['POST'])
def upload_route():
    if 'file' not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado."}), 400
    file = request.files['file']
    response, status = handle_upload(file, project_id=1)  # Exemplo com ID fixo
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
    """Salva dados no fluxo Delta."""
    try:
        data = request.json
        df = spark.createDataFrame(data["data"])
        response = write_to_delta(flow_id, df, data.get("version_name", "default"))
        return jsonify(response), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/flow/<flow_id>/read', methods=['GET'])
def read_flow(flow_id):
    """Lê dados do fluxo Delta."""
    try:
        df = read_from_delta(flow_id)
        return jsonify({"data": df.limit(5).toPandas().to_dict()}), 200
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

if __name__ == '__main__':
    app.run(debug=True)
