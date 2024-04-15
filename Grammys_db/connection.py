import os
import pandas as pd
from sqlalchemy import create_engine
from grammy import grammy_awards
from dotenv import load_dotenv
import pymysql

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_database = os.getenv("DB_DATABASE")

try:
    Workshop_2_mysql_connection_str = f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_database}'
    Workshop_2_mysql_db_connection = create_engine(Workshop_2_mysql_connection_str)
    print("Conexión establecida con la base de datos...")

    # Cargar el DataFrame en la base de datos
    grammy_awards.to_sql('grammy_awards', con=Workshop_2_mysql_db_connection, index=False, if_exists='replace')
    print("Datos cargados correctamente en la tabla 'grammy_awards'.")

except pymysql.Error as e:
    print(f"Fallo al conectar a la base de datos MySQL: {e}")

finally:

    if 'Workshop_2_mysql_db_connection' in locals():
        Workshop_2_mysql_db_connection.dispose()
    print("Conexión de datos y carga realizada correctamente, conexión cerrada.")
