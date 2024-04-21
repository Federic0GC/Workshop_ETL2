import pandas as pd
import pickle
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_dataset(file_path):

    try:
        logging.info("Loading data from CSV file...")
        dataset = pd.read_csv(file_path, delimiter=',')
        logging.info(f"Data loaded successfully from CSV file. Total records: {len(dataset)}")
        logging.info("First rows of the dataset:")
        logging.info(dataset.head())
        return dataset
    except Exception as e:
        logging.error(f"Error loading dataset from CSV file: {e}")
        return None


def transform_dataset(dataset):

    try:
        logging.info("Performing transformations on the dataset...")
        dataset.dropna(inplace=True)
        logging.info(f"Null values dropped. Total records after dropping nulls: {len(dataset)}")
        logging.info("Dataset information after dropping nulls:")
        logging.info(dataset.info())

        dataset.drop_duplicates(inplace=True)
        logging.info(f"Duplicated rows removed. Total records after removing duplicates: {len(dataset)}")
        logging.info("Dataset information after removing duplicates:")
        logging.info(dataset.info())

        if 'Unnamed: 0' in dataset.columns:
            dataset.drop(columns=['Unnamed: 0'], inplace=True)
            logging.info("Column 'Unnamed: 0' dropped from the DataFrame.")
            logging.info("First rows of the dataset after dropping 'Unnamed: 0' column:")
            logging.info(dataset.head())

        columns_to_clean = ['album_name', 'artists', 'track_name']
        for column in columns_to_clean:
            dataset = dataset[dataset[column].str.match(r'^[A-Za-z\s]*$')]
            logging.info(f"Special characters removed in '{column}' column.")
            logging.info(f"First rows of the dataset after removing special characters in '{column}' column:")
            logging.info(dataset.head())

        return dataset
    except Exception as e:
        logging.error(f"Error in dataset transformation process: {e}")
        return None


def save_cleaned_dataset(cleaned_dataset, file_path):

    try:
        logging.info("Saving cleaned dataset as a pickle file...")
        with open(file_path, 'wb') as f:
            pickle.dump(cleaned_dataset, f)
        logging.info("Cleaned dataset saved as a pickle file successfully.")
        logging.info("First rows of the saved dataset:")
        logging.info(cleaned_dataset.head())
        logging.info("Information of the saved dataset:")
        logging.info(cleaned_dataset.info())
    except Exception as e:
        logging.error(f"Error saving cleaned dataset as a pickle file: {e}")


file_path = './Data/spotify_dataset.csv'

dataset = load_dataset(file_path)

if dataset is not None:
    cleaned_dataset = transform_dataset(dataset)

    if cleaned_dataset is not None:
        save_cleaned_dataset(cleaned_dataset, 'spotify_dataset.pkl')
    else:
        logging.error("Error during dataset transformation process.")
else:
    logging.error("Error loading dataset from CSV file.")
