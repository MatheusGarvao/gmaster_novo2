from werkzeug.utils import secure_filename
from services.file_manager import save_file

UPLOAD_FOLDER = "uploads"
ALLOWED_EXTENSIONS = {"zip", "csv", "json", "xml", "xlsx", "txt"}

def allowed_file(filename):
    """Verifica se o arquivo tem uma extensão permitida."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def handle_upload(file, project_id):
    """Processa o upload do arquivo e salva no diretório do projeto."""
    if not allowed_file(file.filename):
        return {"error": "Formato de arquivo não suportado."}, 400

    try:
        project_folder = f"{UPLOAD_FOLDER}/projeto_{project_id}"
        filename = secure_filename(file.filename)
        file_path = save_file(file, project_folder, filename)
        return {"message": "Arquivo salvo com sucesso.", "file_path": file_path}, 200
    except Exception as e:
        return {"error": f"Erro ao salvar o arquivo: {e}"}, 500
