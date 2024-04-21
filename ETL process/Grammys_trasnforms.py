import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pickle
import logging
import pymysql


def load_data_from_database():
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

        return grammy_awards_df

    except Exception as e:
        logging.error(f"Failed to connect to MySQL database: {e}")
        return None


def transform_data(df):
    try:
        logging.info("Checking for null values in the DataFrame...")
        null_counts = df.isnull().sum()
        logging.info("Null value count per column:")
        logging.info(null_counts)

        logging.info("Dropping rows with null values in specific columns 'nominee', 'artist', 'workers', 'img'")
        df.dropna(subset=['nominee', 'artist', 'workers', 'img'], inplace=True)
        logging.info("Transforming columns 'published_at' and 'updated_at'...")
        df['published_at'] = pd.to_datetime(df['published_at'], utc=True)
        df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True)
        
        return df

    except Exception as e:
        logging.error(f"Error during transformation process: {e}")
        return None


def show_transformed_data(df):
    try:
        logging.info("Dataset After Transformations:")
        logging.info(df.head())
        logging.info(df.info())

    except Exception as e:
        logging.error(f"Error displaying transformed data: {e}")


grammy_awards_df = load_data_from_database()

if grammy_awards_df is not None:
    logging.info("Original Dataset:")
    logging.info(grammy_awards_df.head())
    logging.info(grammy_awards_df.info())
    
    transformed_df = transform_data(grammy_awards_df)
    
    if transformed_df is not None:
        show_transformed_data(transformed_df)
        
        with open('grammy_awards_df.pkl', 'wb') as f:
            pickle.dump(transformed_df, f)
            
    else:
        logging.error("Error during transformations.")
else:
    logging.error("Error loading data from the database.")
