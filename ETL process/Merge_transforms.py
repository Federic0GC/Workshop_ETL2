import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_dataframes():
    try:
        logging.info("Cargando dataframes...")
        grammy_df = pd.read_pickle('grammy_awards_df.pkl')
        spotify_df = pd.read_pickle('spotify_dataset.pkl')
        logging.info("Dataframes cargados correctamente.")
        return grammy_df, spotify_df
    except Exception as e:
        logging.error(f"Error al cargar los dataframes: {e}")
        return None, None


def merge_dataframes(grammy_df, spotify_df):
    try:
        logging.info("Realizando merge de los dataframes...")
        merged_df = pd.merge(grammy_df, spotify_df, left_on='artist', right_on='artists', how='inner')
        merged_df['artist'] = merged_df['artist'].combine_first(merged_df['artists'])
       
        valores_unicos = merged_df['winner'].unique()
        logging.info(f"Valores únicos en la columna 'winner': {valores_unicos}")

        conteo_registros = merged_df['winner'].value_counts()
        for valor, conteo in conteo_registros.items():
            logging.info(f"Total de registros con valor '{valor}': {conteo}")

        merged_df.drop(columns=['artists'], inplace=True)
        merged_df = merged_df.drop(columns=['winner'])
        logging.info("Columna 'winner' eliminada del dataset.")

        logging.info("Merge realizado correctamente.")
        return merged_df
    except Exception as e:
        logging.error(f"Error durante el merge de los dataframes: {e}")
        return None


def save_to_database(merged_df, db_user, db_password, db_host, db_database, table_name):
    try:
        Workshop_2_mysql_connection_str = f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_database}'
        Workshop_2_mysql_db_connection = create_engine(Workshop_2_mysql_connection_str)

        logging.info("Guardando dataframe en la base de datos...")
        merged_df.to_sql(table_name, con=Workshop_2_mysql_db_connection, index=False, if_exists='replace')
        logging.info("Datos guardados en la tabla correctamente.")
    except Exception as e:
        logging.error(f"Error al guardar en la base de datos: {e}")
    finally:
        if 'Workshop_2_mysql_db_connection' in locals():
            Workshop_2_mysql_db_connection.dispose()
        logging.info("Conexión cerrada.")


def save_to_csv(dataframe, filename):
    try:
        logging.info("Guardando dataframe como archivo CSV...")
        dataframe.to_csv(filename, index=False)
        logging.info("DataFrame guardado como archivo CSV correctamente.")
    except Exception as e:
        logging.error(f"Error al guardar el DataFrame como archivo CSV: {e}")


grammy_df, spotify_df = load_dataframes()

if grammy_df is not None and spotify_df is not None:
    print("Información del DataFrame Grammy:")
    print(grammy_df.info())
    print("Primeras filas del DataFrame Grammy:")
    print(grammy_df.head())

    print("Información del DataFrame Spotify:")
    print(spotify_df.info())
    print("Primeras filas del DataFrame Spotify:")
    print(spotify_df.head())

    merged_df = merge_dataframes(grammy_df, spotify_df)
    
    if merged_df is not None:
        print("Información del DataFrame fusionado:")
        print(merged_df.info())
        print("Primeras filas del DataFrame fusionado:")
        print(merged_df.head())

        csv_filename = 'transformed_dataset_workshop.csv'
        save_to_csv(merged_df, csv_filename)

        load_dotenv()
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_database = os.getenv("DB_DATABASE")
        table_name = 'workshop_002_merged3'
        
        save_to_database(merged_df, db_user, db_password, db_host, db_database, table_name)

        # Mostrar información y las primeras filas del dataframe guardado en la base de datos
        Workshop_2_mysql_connection_str = f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_database}'
        Workshop_2_mysql_db_connection = create_engine(Workshop_2_mysql_connection_str)
        saved_df = pd.read_sql(f'SELECT * FROM {table_name}', Workshop_2_mysql_db_connection)
        print("Información del DataFrame guardado en la base de datos:")
        print(saved_df.info())
        print("Primeras filas del DataFrame que guardamos en la base de datos:")
        print(saved_df.head())
    else:
        logging.error("Error durante el proceso de merge de los dataframes.")
else:
    logging.error("Error al cargar los dataframes.")
