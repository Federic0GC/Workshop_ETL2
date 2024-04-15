import pandas as pd
import pickle
import logging

# Configuración del registro de eventos
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_dataset(file_path):
    """
    Función para cargar el dataset desde un archivo CSV.
    Devuelve el DataFrame cargado.
    """
    try:
        logging.info("Cargando datos desde el archivo CSV...")
        # Cargar datos desde el archivo CSV
        dataset = pd.read_csv(file_path, delimiter=',')
        logging.info(f"Datos cargados correctamente desde el archivo CSV. Total de registros: {len(dataset)}")
        # Mostrar las primeras filas del dataset
        logging.info("Primeras filas del dataset:")
        logging.info(dataset.head())
        return dataset
    except Exception as e:
        logging.error(f"Error al cargar el dataset desde el archivo CSV: {e}")
        return None


def transform_dataset(dataset):
    """
    Función para realizar todas las transformaciones necesarias en el dataset.
    Devuelve el DataFrame transformado.
    """
    try:
        logging.info("Realizando transformaciones en el dataset...")
        # Eliminar valores nulos
        dataset.dropna(inplace=True)
        logging.info(f"Valores nulos eliminados. Total de registros después de eliminar nulos: {len(dataset)}")
        # Mostrar información del dataset
        logging.info("Información del dataset después de eliminar nulos:")
        logging.info(dataset.info())

        # Eliminar filas duplicadas
        dataset.drop_duplicates(inplace=True)
        logging.info(f"Filas duplicadas eliminadas. Total de registros después de eliminar duplicados: {len(dataset)}")
        # Mostrar información del dataset
        logging.info("Información del dataset después de eliminar duplicados:")
        logging.info(dataset.info())

        # Eliminar columna Unnamed
        if 'Unnamed: 0' in dataset.columns:
            dataset.drop(columns=['Unnamed: 0'], inplace=True)
            logging.info("Columna 'Unnamed: 0' eliminada del DataFrame.")
            # Mostrar las primeras filas del dataset
            logging.info("Primeras filas del dataset después de eliminar columna 'Unnamed: 0':")
            logging.info(dataset.head())

        # Eliminar caracteres especiales en las columnas especificadas
        columns_to_clean = ['album_name', 'artists', 'track_name']
        for column in columns_to_clean:
            dataset = dataset[dataset[column].str.match(r'^[A-Za-z\s]*$')]
            logging.info(f"Caracteres especiales eliminados en la columna '{column}'.")
            # Mostrar las primeras filas del dataset
            logging.info(f"Primeras filas del dataset después de eliminar caracteres especiales en la columna '{column}':")
            logging.info(dataset.head())

        return dataset
    except Exception as e:
        logging.error(f"Error en el proceso de transformación del dataset: {e}")
        return None


def save_cleaned_dataset(cleaned_dataset, file_path):
    """
    Función para guardar el dataset limpio como un archivo pickle.
    """
    try:
        logging.info("Guardando dataset limpio como archivo pickle...")
        with open(file_path, 'wb') as f:
            pickle.dump(cleaned_dataset, f)
        logging.info("Dataset limpio guardado como archivo pickle correctamente.")
        # Mostrar las primeras filas del dataset guardado
        logging.info("Primeras filas del dataset guardado:")
        logging.info(cleaned_dataset.head())
        # Mostrar información del dataset guardado
        logging.info("Información del dataset guardado:")
        logging.info(cleaned_dataset.info())
    except Exception as e:
        logging.error(f"Error al guardar el dataset limpio como archivo pickle: {e}")


# Ruta del archivo CSV
file_path = './Data/spotify_dataset.csv'

# Leer el dataset desde el archivo CSV
dataset = load_dataset(file_path)

if dataset is not None:
    # Realizar las transformaciones en el dataset
    cleaned_dataset = transform_dataset(dataset)

    if cleaned_dataset is not None:
        # Guardar el dataset limpio como archivo pickle
        save_cleaned_dataset(cleaned_dataset, 'spotify_dataset.pkl')
    else:
        logging.error("Error durante el proceso de transformación del dataset.")
else:
    logging.error("Error al cargar el dataset desde el archivo CSV.")
