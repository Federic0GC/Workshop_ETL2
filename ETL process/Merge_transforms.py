import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_dataframes():
    try:
        logging.info("Loading dataframes...")
        grammy_df = pd.read_pickle('grammy_awards_df.pkl')
        spotify_df = pd.read_pickle('spotify_dataset.pkl')
        logging.info("Dataframes loaded successfully.")
        return grammy_df, spotify_df
    except Exception as e:
        logging.error(f"Error loading dataframes: {e}")
        return None, None


def merge_dataframes(grammy_df, spotify_df):
    try:
        logging.info("Merging dataframes...")
        merged_df = pd.merge(grammy_df, spotify_df, left_on='artist', right_on='artists', how='inner')
        merged_df['artist'] = merged_df['artist'].combine_first(merged_df['artists'])
       
        valores_unicos = merged_df['winner'].unique()
        logging.info(f"Unique values in 'winner' column: {valores_unicos}")

        conteo_registros = merged_df['winner'].value_counts()
        for valor, conteo in conteo_registros.items():
            logging.info(f"Total records with value '{valor}': {conteo}")

        merged_df.drop(columns=['artists'], inplace=True)
        merged_df = merged_df.drop(columns=['winner'])
        logging.info("Column 'winner' removed from the dataset.")

        logging.info("Merge completed successfully.")
        return merged_df
    except Exception as e:
        logging.error(f"Error during dataframe merge: {e}")
        return None


def save_to_database(merged_df, db_user, db_password, db_host, db_database, table_name):
    try:
        Workshop_2_mysql_connection_str = f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_database}'
        Workshop_2_mysql_db_connection = create_engine(Workshop_2_mysql_connection_str)

        logging.info("Saving dataframe to the database...")
        merged_df.to_sql(table_name, con=Workshop_2_mysql_db_connection, index=False, if_exists='replace')
        logging.info("Data saved to the table successfully.")
    except Exception as e:
        logging.error(f"Error saving to the database: {e}")
    finally:
        if 'Workshop_2_mysql_db_connection' in locals():
            Workshop_2_mysql_db_connection.dispose()
        logging.info("Connection closed.")


def save_to_csv(dataframe, filename):
    try:
        logging.info("Saving dataframe as CSV file...")
        dataframe.to_csv(filename, index=False)
        logging.info("DataFrame saved as CSV file successfully.")
    except Exception as e:
        logging.error(f"Error saving DataFrame as CSV file: {e}")


grammy_df, spotify_df = load_dataframes()

if grammy_df is not None and spotify_df is not None:
    print("DataFrame Grammy Information:")
    print(grammy_df.info())
    print("First rows of DataFrame Grammy:")
    print(grammy_df.head())

    print("DataFrame Spotify Information:")
    print(spotify_df.info())
    print("First rows of DataFrame Spotify:")
    print(spotify_df.head())

    merged_df = merge_dataframes(grammy_df, spotify_df)
    
    if merged_df is not None:
        print("Merged DataFrame Information:")
        print(merged_df.info())
        print("First rows of Merged DataFrame:")
        print(merged_df.head())

        csv_filename = 'transformed_dataset_workshop.csv'
        save_to_csv(merged_df, csv_filename)

        load_dotenv()
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_database = os.getenv("DB_DATABASE")
        table_name = 'workshop_002_merged'
        
        save_to_database(merged_df, db_user, db_password, db_host, db_database, table_name)

        Workshop_2_mysql_connection_str = f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_database}'
        Workshop_2_mysql_db_connection = create_engine(Workshop_2_mysql_connection_str)
        saved_df = pd.read_sql(f'SELECT * FROM {table_name}', Workshop_2_mysql_db_connection)
        print("Information of the DataFrame saved in the database:")
        print(saved_df.info())
        print("First rows of the DataFrame we saved in the database:")
        print(saved_df.head())
    else:
        logging.error("Error during the dataframe merge process.")
else:
    logging.error("Error loading the dataframes.")
