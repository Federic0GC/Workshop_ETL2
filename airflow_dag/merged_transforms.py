import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

def dataset_merged_spotify_and_grammy(**kwargs):
    try:
        ti = kwargs["ti"]
        grammy_df_str = ti.xcom_pull(task_ids="transform_grammy_data")
        grammy_df = pd.read_json(grammy_df_str, orient='records')

        spotify_df_str = ti.xcom_pull(task_ids="clean_spotify_data")
        spotify_df = pd.read_json(spotify_df_str, orient='records')
        
        merged_df = pd.merge(grammy_df, spotify_df, left_on='artist', right_on='artists', how='inner')

        logging.info("Head of merged DataFrame:")
        logging.info(merged_df.head())
        logging.info("Info of merged DataFrame:")
        logging.info(merged_df.info())
        
        csv_filename = 'transformed_dataset_workshop.csv'
        logging.info(f"Saving merged DataFrame as CSV file: {csv_filename}")
        merged_df.to_csv(csv_filename, index=False)
        logging.info("DataFrame saved as CSV file successfully.")
        
        return merged_df.to_json(orient='records')
    
    except Exception as e:
        logging.error(f"Error in the dataset merging process: {e}")
        return None
