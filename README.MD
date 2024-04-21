
# Workshop_ETL 2

## Context

### Workshop 2: ETL Process using airflow
### This project focuses on an ETL process that will be demonstrated and executed in a pipeline using Apache Airflow. The idea is to extract two dataframes, one from a CSV file (Spotify Dataset) and another dataset extracted from a database connection, storing the dataset first in a database and then connecting to the database to extract the dataframe (Grammys Awards Dataset).

### Both extracted datasets will undergo a cleaning process called EDA. Then, the datasets will need to be combined in such a way that the new dataset has value from both datasets, either by complementing one dataset with the other or by merging with a specific field that both datasets have.

### This new dataset will undergo its own analysis and will be the final product that will be stored both in Drive, in the database, and will be used to create the dashboard for this workshop.

- Python
- Jupyter notebook
- CSV files
- MySQL Workbench
- Apache Airflow
- PowerBI
- Docker


## Steps to use and clone this repository
## 1. Clone this repository to your system
```git clone https://github.com/Federic0GC/Workshop_ETL1.git```
## 2. Make sure to install MySQL Workbench and Python on your system
## Apache Airflow Installation
## Linux: Create a virtual environment on your system
``` python -m venv nombre_de_tu_entorno```
### Make sure you are located in your virtual environment and install Airflow using pip
``` pip install apache-airflow```
### Install the requirements in your virtual environment from "requirements.txt"
``` pip install -r requirements.txt```
### Define the AIRFLOW_HOME variable as the root of the project (you should be located within it)
``` export AIRFLOW_HOME=$(pwd) ```
### Start Airflow for the first time with the following command, which will set up a standard admin configuration with a username and password
``` airflow standalone ```
### Congratulations! After starting, you should be able to work with Airflow by accessing the Airflow panel with your credentials
![Airflow](https://cdn.discordapp.com/attachments/1068002173448896532/1231704940196659342/df6a2048-549a-46f6-9d5e-8dff3da087f1.jpeg?ex=6637ed9d&is=6625789d&hm=87171ebfb229b8a7f18f4534a3006689f0e638cb6be1fb169407eac1aa7849c0&)





