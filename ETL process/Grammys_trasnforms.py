import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pickle
import logging
import pymysql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data_from_database():
    """
    Función para establecer la conexión con la base de datos y cargar los datos en un DataFrame.
    Devuelve el DataFrame cargado con los datos.
    """
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

        return grammy_awards_df

    except Exception as e:
        logging.error(f"Fallo al conectar a la base de datos MySQL: {e}")
        return None


def transform_data(df):
    """
    Función para realizar las transformaciones necesarias en el DataFrame.
    Devuelve el DataFrame transformado.
    """
    try:
        logging.info("Buscando valores nulos en el DataFrame...")
        null_counts = df.isnull().sum()
        logging.info("Recuento de valores nulos por columna:")
        logging.info(null_counts)

        logging.info("Eliminando filas con valores nulos en columnas específicas 'nominee', 'artist', 'workers', 'img'")
        df.dropna(subset=['nominee', 'artist', 'workers', 'img'], inplace=True)
        logging.info("Transformando las columnas 'published_at' y 'updated_at'...")
        df['published_at'] = pd.to_datetime(df['published_at'], utc=True)
        df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True)
        
        return df

    except Exception as e:
        logging.error(f"Error en el proceso de transformación: {e}")
        return None


def show_transformed_data(df):
    try:
        logging.info("Dataset PostTransformaciones:")
        logging.info(df.head())
        logging.info(df.info())

    except Exception as e:
        logging.error(f"Error al mostrar los datos transformados: {e}")


grammy_awards_df = load_data_from_database()

if grammy_awards_df is not None:
    logging.info("Dataset original:")
    logging.info(grammy_awards_df.head())
    logging.info(grammy_awards_df.info())
    
    transformed_df = transform_data(grammy_awards_df)
    
    if transformed_df is not None:
        show_transformed_data(transformed_df)
        
        with open('grammy_awards_df.pkl', 'wb') as f:
            pickle.dump(transformed_df, f)
            
    else:
        logging.error("Error durante las transformaciones.")
else:
    logging.error("Error al cargar los datos desde la base de datos.")
