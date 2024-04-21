import logging
import json
import pandas as pd

def read_spotify_data():
    try:
       
        logging.info("Leyendo el csv...")
        
        
        spotify_dataset = pd.read_csv('./Data/spotify_dataset.csv', delimiter=',') 
        logging.info("Extracción de datos completada.")
        logging.debug('Datos extraídos son: ', spotify_dataset)
        
        
        json_data = spotify_dataset.to_json(orient='records')
        
        
        logging.info("Lectura del csv completada.")
        
        return json_data
    except Exception as e:
        logging.error(f"Error al leer el archivo CSV: {e}")
        return None

def clean_spotify_data(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_spotify_data")
    
    if str_data:
        try:
            json_data = json.loads(str_data)
            df = pd.json_normalize(data=json_data)
            
            
            df = df.dropna()
            logging.info("Se eliminaron las filas con valores NaN del DataFrame.")
            
           
            if 'Unnamed: 0' in df.columns:
                df = df.drop(columns=['Unnamed: 0'])
                logging.info("Se eliminó la columna 'Unnamed: 0' del DataFrame.")
                
           
            df.drop_duplicates(inplace=True)
            logging.info("Se eliminaron las filas duplicadas del DataFrame.")
            
            return df.to_json(orient='records')
        except Exception as e:
            logging.error(f"Error en el proceso de limpieza: {e}")
            return None
    else:
        logging.error("No se recibieron datos válidos de la tarea read_spotify_data.")
        return None



# Llama a la función read_spotify_data() para obtener el DataFrame
#spotify_df = read_spotify_data()

# Llama a la función clean_spotify_data() para realizar las operaciones de limpieza
#cleaned_spotify_df = clean_spotify_data(spotify_df)

# Muestra el DataFrame limpio
#print(cleaned_spotify_df)
#print(cleaned_spotify_df.info())





# Funciones asesoria

# def read_spotify_csv():
#  Spotify_dataset = pd.read_csv('/home/federico/airflow-docker/dags/Workshop_ETL2/Data/spotify_dataset.csv', delimiter=',') 
#  logging.info("Extraccion de datos...")
#  logging.debug('Datos extraidos son: ', Spotify_dataset)
#  return Spotify_dataset.to_json(orient='records')


# def transform_csv(**kwargs):
#    ti = kwargs["ti"]
#    json_data = json.loads(ti.xcom_pull(task_ids="read_spotify_data"))
#    Spotify_dataset = pd.json_normalize(data=json_data)
   
#    logging.info("Los datos transformados son: {Spotify_dataset}")
    #return Spotify_dataset.to_json(orient='records')