import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
import logging

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_database = os.getenv("DB_DATABASE")

def save_merge_to_database(**kwargs):
    try:
        ti = kwargs["ti"]
        merged_data_str = ti.xcom_pull(task_ids="dataset_merged")
        
        if merged_data_str:
            merged_data_df = pd.read_json(merged_data_str, orient='records')
            
            connection_str = f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_database}'
            engine = create_engine(connection_str)
            
            merged_data_df.to_sql('workshop_002_merged', con=engine, if_exists='replace', index=False)
            
            logging.info("Merge saved successfully in the 'workshop_002_merged' table.")
        else:
            logging.info("No valid data received from the merge.")
    except Exception as e:
        logging.error(f"Error while saving the merge to the database: {e}")
