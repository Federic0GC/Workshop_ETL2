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
    print("Connection established with the database...")

    grammy_awards.to_sql('grammy_awards', con=Workshop_2_mysql_db_connection, index=False, if_exists='replace')
    print("Data successfully loaded into the 'grammy_awards' table.")

except pymysql.Error as e:
    print(f"Failed to connect to MySQL database: {e}")

finally:
    if 'Workshop_2_mysql_db_connection' in locals():
        Workshop_2_mysql_db_connection.dispose()
    print("Data connection and loading completed successfully, connection closed.")
