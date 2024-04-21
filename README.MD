# Workshop_ETL 2 

## Contexto

### Workshop 2: ETL Process using airflow

### Este proyecto se enfoca en un proceso de ETL que se mostrara y ejecutara en un pipeline usando Apache Airflow, la idea es extraer dos dataframes, uno que se trabajara extrayendolo desde un csv (Spotify Dataset) y otro dataset que se va a extraer desde una conexion con una base de datos, guardando el dataset primero en una base de datos para despues conectarse a la base de datos y extraer el datframe ( Grammys Awards Dataset). 

### Los dos datasets ya extraidos pasaran cada uno por un proceso de limpieza llamado EDA, despues los datasets se tendran que combinar de tal forma que el nuevo dataset tenga valor de ambos datasets, ya sean complementando un dataset con el otro o hacer un merge con un campo esecifico que tengan los do datasets. 

### Este nuevo dataset tendra su propio analisis y sera el producto final que se guardara tanto en drive, en la base de datos y con el que se hara el dashboard de este workshop.

## Herramientas utlizadas para la relizacion del proyecto
- Python
- Jupyter notebook
- CSV files
- MySQL Workbench
- Apache Airflow
- PowerBI
- Docker

## Pasos para utlizar y clonar este repositorio
## 1. Clona este repositorio en tu sistema 
```git clone https://github.com/Federic0GC/Workshop_ETL1.git```
## 2. Asegurate de instalarte en tu sistema las herramientas MySQL Workbench y Python
## Instalacion de Apache Airflow
## Linux: Crea un entorno virtual en tu sistema
``` python -m venv nombre_de_tu_entorno```
### Aegurate de estar ubicado en tu entorno virtual e instala airflow con pip
``` pip install apache-airflow```
### instala los requerimeitnos en tu entorno virtual "requeriments.txt"
``` pip install -r requeriments.txt```
### Define la variable AIRFLOW_HOME como la raiz del proyecto (debes estar ubicada en ella)
``` export AIRFLOW_HOME=$(pwd) ```
### Inicia airflow por primera vez con el siguiente comando que te definiria una configuracion estandar de admin como usuario y una contraseña 
``` airflow standalone ```
### Enhorabuena, Ya deberias despues de iniciar estar en el panel de airflow con tus credenciales poder trabajar con airflow
![Airflow](https://cdn.discordapp.com/attachments/1068002173448896532/1231704940196659342/df6a2048-549a-46f6-9d5e-8dff3da087f1.jpeg?ex=6637ed9d&is=6625789d&hm=87171ebfb229b8a7f18f4534a3006689f0e638cb6be1fb169407eac1aa7849c0&)





