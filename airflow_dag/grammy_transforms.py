import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

def extract_db_to_dataframe():
    try:
        load_dotenv()
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_database = os.getenv("DB_DATABASE")
        
        mysql_connection_str = f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_database}'
        db_connection = create_engine(mysql_connection_str)
        logging.info("Conexión establecida con la base de datos...")

        table_name = 'grammy_awards'
        query = f'SELECT * FROM {table_name}'
        grammy_awards_df = pd.read_sql(query, con=db_connection)
        
        return grammy_awards_df.to_json(orient='records')

    except Exception as e:
        logging.error(f"Fallo al conectar a la base de datos MySQL: {e}")
        return None
    
def transform_dataset_grammy(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="extract_db_to_dataframe")
    
    if str_data:
        try:
            grammy_awards_df = pd.read_json(str_data, orient='records')
            grammy_awards_df.dropna(subset=['nominee', 'artist', 'workers', 'img'], inplace=True)
            logging.info("Se eliminaron registros con valores nulos en columnas específicas.")
            if 'Unnamed: 0' in grammy_awards_df.columns:
                grammy_awards_df.drop(columns=['Unnamed: 0'], inplace=True)
                logging.info("Se eliminó la columna 'Unnamed: 0' del DataFrame.")
            grammy_awards_df['published_at'] = pd.to_datetime(grammy_awards_df['published_at'], utc=True)
            grammy_awards_df['updated_at'] = pd.to_datetime(grammy_awards_df['updated_at'], utc=True)
            logging.info("Se cambió el tipo de dato de las columnas 'published_at' y 'updated_at' a datetime.")
            return grammy_awards_df.to_json(orient='records')
        except Exception as e:
            logging.error(f"Error en el proceso de transformación: {e}")
            return None
    else:
        logging.error("No se recibieron datos válidos de la tarea extract_db_to_dataframe.")
        return None


