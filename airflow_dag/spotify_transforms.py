import logging
import json
import pandas as pd

def read_spotify_data_csv():
    try:
       
        logging.info("Reading the csv file...")
        
        
        spotify_dataset = pd.read_csv('./Data/spotify_dataset.csv', delimiter=',') 
        logging.info("Data extraction completed.")
        logging.debug('Extracted data is: ', spotify_dataset)
        
        
        json_data = spotify_dataset.to_json(orient='records')
        
        
        logging.info("CSV file read completed.")
        
        return json_data
    except Exception as e:
        logging.error(f"Error while reading the CSV file: {e}")
        return None

def clean_spotify_dataset(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_spotify_data")
    
    if str_data:
        try:
            json_data = json.loads(str_data)
            df = pd.json_normalize(data=json_data)
            
            
            df = df.dropna()
            logging.info("Rows with NaN values removed from the DataFrame.")
            
           
            if 'Unnamed: 0' in df.columns:
                df = df.drop(columns=['Unnamed: 0'])
                logging.info("Column 'Unnamed: 0' removed from the DataFrame.")
                
           
            df.drop_duplicates(inplace=True)
            logging.info("Duplicate rows removed from the DataFrame.")
            
            return df.to_json(orient='records')
        except Exception as e:
            logging.error(f"Error during the cleaning process: {e}")
            return None
    else:
        logging.error("No valid data received from the read_spotify_data task.")
        return None
