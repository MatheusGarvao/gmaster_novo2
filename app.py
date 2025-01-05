from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from database_manager import DatabaseConnectionManager
from functions import (
    replace_value,
    transpor,
    rename_column,
    calcular_nova_coluna,
    sumarizar,
    calcular_media_ponderada   
)
from database_manager import DatabaseConnectionManager
from conector import process_zip, process_excel, process_json, process_xml, process_csv, load_dataframe, process_txt

app = Flask(__name__, template_folder="templates")

db_manager = DatabaseConnectionManager()

global_df = None  

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/database', methods=['POST'])
def handle_database_request():
    global global_df
    try:
        data = request.json
        if not data:
            return jsonify({"error": "Dados não fornecidos na requisição."}), 400
        
        action = data.get("action")
        if action not in ["set_database", "load_table"]:
            return jsonify({"error": "Ação inválida. Use 'set_database' ou 'load_table'."}), 400
        
        if action == "set_database":
            db_type = data.get("db_type")
            if not db_type:
                return jsonify({"error": "Tipo de banco de dados não especificado."}), 400
            db_manager.configure_connection(db_type)
            return jsonify({
                "message": f"Conexão configurada com sucesso para {db_type}",
                "db_type": db_type
            })
        
        elif action == "load_table":
            table_name = data.get("table_name")
            if not table_name:
                return jsonify({"error": "Nome da tabela não fornecido."}), 400
            data = db_manager.load_table_data(table_name)
            global_df = load_dataframe(data)
            return jsonify({
                "message": f"Dados carregados com sucesso da tabela '{table_name}'",
                "data": data,
                "row_count": len(data)
            })
    
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Erro inesperado: {str(e)}"}), 500

@app.route('/upload', methods=['POST'])
def upload_file():
    global global_df  
    file = request.files.get('file')
    if not file:
        return jsonify({"error": "Nenhum arquivo enviado"}), 400

    try:
        if file.filename.endswith('.zip'):
            data = process_zip(file)  # Processa arquivos ZIP
        elif file.filename.endswith('.csv'):
            data = process_csv(file)  # Processa arquivos CSV
        elif file.filename.endswith('.json'):
            data = process_json(file)  # Processa arquivos JSON
        elif file.filename.endswith('.xml'):
            data = process_xml(file)  # Processa arquivos XML
        elif file.filename.endswith('.xlsx'):
            data = process_excel(file)  # Processa arquivos Excel
        elif file.filename.endswith('.txt'):
            data = process_txt(file)
        else:
            return jsonify({"error": "Formato de arquivo não suportado."}), 400

        # Converte os dados processados para DataFrame Polars
        global_df = load_dataframe(data)
        return jsonify({
            "message": "Dados carregados com sucesso.",
            "data": data  # Dados processados do arquivo
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/replace_value', methods=['POST'])    
def handle_replace_value():
    global global_df
    try:
        global_df, error = replace_value(global_df, request)
        if error:  # Se houve um erro na função replace_value
            return jsonify(error), 400
        return jsonify(global_df.to_dicts()), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/transpor', methods=['POST'])
def handle_transpor():
    global global_df
    response = transpor(global_df, request)
    return response

@app.route('/rename_column', methods=['POST'])
def handle_rename_column():
    global global_df
    try:
        global_df, error = rename_column(global_df, request)
        if error:
            return jsonify(error), 400
        return jsonify(global_df.to_dicts())  # Retorna o DataFrame atualizado para o frontend
    except ValueError as e:
        return jsonify({"error": str(e)}), 500

@app.route('/calcular_nova_coluna', methods=['POST'])
def handle_calcular_nova_coluna():
    global global_df
    try:
        global_df, error = calcular_nova_coluna(global_df, request)
        if error:
            return jsonify(error), 400
        return jsonify(global_df.to_dicts())  # Retorna o DataFrame atualizado para o frontend
    except ValueError as e:
        return jsonify({"error": str(e)}), 500
 
@app.route('/sumarizar', methods=['POST'])
def handle_sumarizar():
    global global_df
    try:
        global_df, error = sumarizar(global_df, request)
        if error:
            return jsonify(error), 400
        return jsonify(global_df.to_dicts())  # Retorna o DataFrame atualizado para o frontend
    except ValueError as e:
        return jsonify({"error": str(e)}), 500

@app.route('/calcular_media_ponderada', methods=['POST'])
def handle_media_ponderada():
    global global_df
    try:
        global_df, error = calcular_media_ponderada(global_df, request)
        if error:
            return jsonify(error), 400
        return jsonify(global_df.to_dicts())  # Retorna o DataFrame atualizado para o frontend
    except ValueError as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    CORS(app, origins="http://localhost:3000")
    app.run(host='0.0.0.0', port=5000, debug=True)