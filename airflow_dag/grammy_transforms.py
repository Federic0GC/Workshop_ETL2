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
        logging.info("Connection established with the database...")

        table_name = 'grammy_awards'
        query = f'SELECT * FROM {table_name}'
        grammy_awards_df = pd.read_sql(query, con=db_connection)
        
        return grammy_awards_df.to_json(orient='records')

    except Exception as e:
        logging.error(f"Failed to connect to the MySQL database: {e}")
        return None
    
def transform_dataset_grammy(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="extract_db_to_dataframe")
    
    if str_data:
        try:
            grammy_awards_df = pd.read_json(str_data, orient='records')
            grammy_awards_df.dropna(subset=['nominee', 'artist', 'workers', 'img'], inplace=True)
            logging.info("Records with null values in specific columns were removed.")
            if 'Unnamed: 0' in grammy_awards_df.columns:
                grammy_awards_df.drop(columns=['Unnamed: 0'], inplace=True)
                logging.info("Column 'Unnamed: 0' was removed from the DataFrame.")
            grammy_awards_df['published_at'] = pd.to_datetime(grammy_awards_df['published_at'], utc=True)
            grammy_awards_df['updated_at'] = pd.to_datetime(grammy_awards_df['updated_at'], utc=True)
            logging.info("Data type of columns 'published_at' and 'updated_at' was changed to datetime.")
            return grammy_awards_df.to_json(orient='records')
        except Exception as e:
            logging.error(f"Error in the transformation process: {e}")
            return None
    else:
        logging.error("No valid data received from the extract_db_to_dataframe task.")
        return None

