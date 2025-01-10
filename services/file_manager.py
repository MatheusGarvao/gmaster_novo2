import os
import polars as pl
import json
from io import BytesIO, StringIO
import zipfile

def save_file(file, directory, filename):
    """Salva o arquivo no diretório especificado."""
    os.makedirs(directory, exist_ok=True)
    file_path = os.path.join(directory, filename)
    file.save(file_path)
    return file_path

def extract_zip(file, output_dir):
    """Extrai arquivos ZIP para o diretório de saída."""
    os.makedirs(output_dir, exist_ok=True)
    with zipfile.ZipFile(BytesIO(file.read()), 'r') as zip_ref:
        zip_ref.extractall(output_dir)
    return [os.path.join(output_dir, name) for name in os.listdir(output_dir)]

def read_file(file_path):
    """Lê o arquivo e retorna um DataFrame Polars."""
    if file_path.endswith('.csv'):
        return pl.read_csv(file_path)
    elif file_path.endswith('.json'):
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return pl.DataFrame(data)
    elif file_path.endswith('.xlsx'):
        return pl.read_excel(file_path)
    elif file_path.endswith('.txt'):
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = [line.strip().split('|') for line in f.readlines()]
            return pl.DataFrame(lines)
    else:
        raise ValueError(f"Formato de arquivo não suportado: {file_path}")
    
def delete_uploaded_file(filename):
    """Exclui um arquivo enviado com base no nome."""
    UPLOAD_FOLDER = "uploads"
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    if os.path.exists(file_path):
        os.remove(file_path)
        return {"message": f"Arquivo {filename} deletado com sucesso."}
    else:
        raise FileNotFoundError(f"Arquivo {filename} não encontrado.")
