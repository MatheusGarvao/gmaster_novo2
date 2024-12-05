from flask import Flask, render_template, request, jsonify
from flask_session import Session
import pandas as pd
import os
import numpy as np
import xml.etree.ElementTree as ET
from io import StringIO, BytesIO
import chardet
import re
import zipfile
from database_manager import DatabaseConnectionManager
from conector import process_zip, process_excel, process_json, process_xml, process_csv

app = Flask(__name__, template_folder="templates")

db_manager = DatabaseConnectionManager()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/database', methods=['POST'])
def handle_database_request():
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
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files['file']
    try:
        if file.filename.endswith('.zip'):
            data = process_zip(file)
        elif file.filename.endswith('.xlsx'):
            data = process_excel(file)
        elif file.filename.endswith('.json'):
            data = process_json(file)
        elif file.filename.endswith('.xml'):
            data = process_xml(file)
        elif file.filename.endswith('.csv'):
            data = process_csv(file)
        else:
            return jsonify({"error": "File type not supported"}), 400

        return jsonify(data)

    except Exception as e:
        print(f"Erro ao processar o arquivo: {e}")
        return jsonify({"error": f"Failed to process the file: {str(e)}"}), 500
    
@app.route('/calcular_nova_coluna', methods=['POST'])
def calcular_nova_coluna():
    global df   
    data = request.json
    print(data.get('formula'))
    print(data.get('new_column'))
    df= pd.DataFrame(data['data'])
    formula = data.get('formula')
    new_column_name = data.get('new_column')
    print(formula)

    if not formula:
        return jsonify({"error": "Fórmula não fornecida"}), 400
    if not new_column_name:
        new_column_name = f"{formula} (Nova)"

    # Substituir os nomes das colunas por df['coluna']
    for col in df.columns:
        if col in formula:
            # Converter a coluna para tipo numérico, substituindo erros por NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            formula = formula.replace(col, f"df['{col}']")
    
    try:
        # Avaliar a fórmula
        df[new_column_name] = eval(formula)

        # Converter NaN para "null" para compatibilidade JSON
        data = df.fillna("null").to_dict(orient='records')
        return jsonify(data)
    except Exception as e:
        print("Erro ao aplicar fórmula:", str(e))
        return jsonify({"error": str(e)}), 500
    

@app.route('/transpor', methods=['POST'])
def transpor():
    global df
    try:
        data = request.get_json()
        
        if 'data' not in data:
            return jsonify({"error": "Dados não fornecidos."}), 400
        
        # Cria um DataFrame a partir dos dados recebidos
        df = pd.DataFrame(data['data'])
        
        columns_as_first_row = pd.DataFrame([df.columns.tolist()], columns=df.columns)
        
        # Adiciona os nomes das colunas como a primeira linha
        df = pd.concat([columns_as_first_row, df], ignore_index=True)
        
        df_transposto = df.T
        result = df_transposto.to_dict(orient='records')
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/rename_column', methods=['POST'])
def rename_column():
    global df
    data = request.json
    current_column = data.get('currentColumn')
    new_column_name = data.get('newColumnName')
    df = pd.DataFrame(data['rawData'])

    if not current_column or not new_column_name:
        return jsonify({"error": "Nome atual e novo nome são necessários."}), 400

    # Verifique se a coluna atual existe no DataFrame
    if current_column not in df.columns:
        return jsonify({"error": f"A coluna '{current_column}' não existe."}), 400

    # Renomeia a coluna
    df.rename(columns={current_column: new_column_name}, inplace=True)
    # save_state(df, f"Renomeou coluna '{current_column}' para '{new_column_name}'")
    print(df.head())
    # Retorne o DataFrame atualizado
    data = df.fillna("null").to_dict(orient='records')
    return jsonify(data)

@app.route('/replace_value', methods=['POST'])
def replace_value():
    global df
    data = request.json
    column = data.get('column')
    old_value = data.get('oldValue')
    new_value = data.get('newValue')
    df = pd.DataFrame(data['data'])
    print(column, old_value, new_value)

    # Verifica se os parâmetros estão presentes
    if not column or old_value is None or new_value is None:
        return jsonify({"error": "Parâmetros incompletos"}), 400

    try:
        # Converte os valores para numéricos (inteiros ou flutuantes)
        old_value = float(old_value)
        new_value = float(new_value)

        # Converte a coluna para tipo numérico antes de substituir os valores
        df[column] = pd.to_numeric(df[column], errors='coerce')

        # Substitui o valor antigo pelo novo
        df[column] = df[column].replace(old_value, new_value)

        print(df.head())
        
        # Preenche valores NaN com "null" para compatibilidade JSON
        updated_data = df.fillna("null").to_dict(orient='records')
        return jsonify(updated_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500



if __name__ == '__main__':
    app.run(debug=True)
    
    
