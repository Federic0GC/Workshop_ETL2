from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import logging

def authenticate():
    gauth = GoogleAuth()
    gauth.LoadClientConfigFile("airflow_dag/client_secret.json")
    gauth.LocalWebserverAuth()
    return gauth

def upload_to_drive(**kwargs):
    try:
        ti = kwargs["ti"]
        file_path = ti.xcom_pull(task_ids="save_merge_to_database")

        if file_path:
            gauth = authenticate()
            drive = GoogleDrive(gauth)

            file_name = file_path.split('/')[-1]
            file = drive.CreateFile({'title': file_name})
            file.SetContentFile(file_path)
            file.Upload()

            logging.info(f"Archivo '{file_name}' subido correctamente a Google Drive.")
        else:
            logging.info("No se recibió el archivo para subir a Google Drive.")
    except Exception as e:
        logging.error(f"Error al subir el archivo a Google Drive: {e}")
