from datetime import datetime
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from spotify_transforms import clean_spotify_dataset, read_spotify_data_csv
from grammy_transforms import extract_db_to_dataframe, transform_dataset_grammy
from merged_transforms import dataset_merged_spotify_and_grammy
from save_to_databse import save_merge_to_database
from upload_to_drive import upload_to_drive


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 20),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}


with DAG('Workshop_002_pipeline', default_args=default_args, schedule_interval=None) as dag:
   
    
    read_spotify_data = PythonOperator(
        task_id='read_spotify_data',
        python_callable=read_spotify_data_csv,
        provide_context=True
    )
    
    
    clean_spotify_data = PythonOperator(
        task_id='clean_spotify_data',
        python_callable=clean_spotify_dataset,
        provide_context=True  
    )
    
    
    extract_grammy_data = PythonOperator(
        task_id='extract_grammy_data',
        python_callable=extract_db_to_dataframe,
        provide_context=True
    )
    
    
    transform_grammy_data = PythonOperator(
        task_id='transform_grammy_data',
        python_callable=transform_dataset_grammy,
        provide_context=True  
    )

    dataset_merged = PythonOperator(
      task_id= 'dataset_merged',
      python_callable=dataset_merged_spotify_and_grammy
    
    )

    save_dataset_merged_db = PythonOperator(
        task_id='save_merge_to_database',
        python_callable=save_merge_to_database,
        provide_context=True
    )

    upload_dataset_merged_drive = PythonOperator(
    task_id='upload_to_drive',
    python_callable=upload_to_drive,
    provide_context=True
    )


    read_spotify_data >> clean_spotify_data >> dataset_merged
    extract_grammy_data >> transform_grammy_data >> dataset_merged
    dataset_merged >> save_dataset_merged_db >> upload_dataset_merged_drive

