from datetime import timedelta,datetime
from pathlib import Path
import json
import requests
import psycopg2
from airflow import DAG
from sqlalchemy import create_engine
# Operadores
from airflow.operators.python_operator import PythonOperator
#from airflow.utils.dates import days_ago
import pandas as pd
import os

dag_path = os.getcwd()     #path original.. home en Docker

url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
data_base= 'data-engineer-database'
user='nachosachetto1998_coderhouse'
pwd= '7obT4RE7uI'

redshift_conn = {
    'host': url,
    'username': user,
    'database': data_base,
    'port': '5439',
    'pwd': pwd
}

default_args = {
    'owner': 'NacchoSachetto',
    'start_date': datetime(2023,12,20),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

futbol_dag = DAG(
    dag_id='ETLFutbol',
    default_args=default_args,
    description='Agrega datos de los partidos de las principales ligas del mundo diariamente.',
    schedule_interval="@daily",
    catchup=False
)


dag_path = os.getcwd()     #path original.. home en Docker

def extraer_data(exec_date):
    try:
        print(f"Adquiriendo data para la fecha: {exec_date}")
        date = datetime.strptime(exec_date, '%Y-%m-%d %H')

        # Crear el directorio 'raw_data' si no existe
        raw_data_dir = os.path.join(dag_path, 'raw_data')
        os.makedirs(raw_data_dir, exist_ok=True)

        file_path = os.path.join(raw_data_dir, f"data_{date.year}-{date.month}-{date.day}-{date.hour}.json")

        url = "https://api.football-data.org/v4/matches"
        headers = {"Accept-Encoding": "gzip, deflate"}
        response = requests.get(url, headers=headers)

        if response:
            print('Success!')
            data = response.json()

            with open(file_path, "w") as json_file:
                json.dump(data, json_file)
        else:
            print('An error has occurred.')
    except ValueError as e:
        print("Formato datetime debería ser %Y-%m-%d %H", e)
        raise e


def transformar_data(exec_date):       
    print(f"Transformando la data para la fecha: {exec_date}") 
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    with open(dag_path+'/raw_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "r") as json_file:
        loaded_data = json.load(json_file)

    # Verificar si la clave 'data' está presente en la estructura de datos
    if 'data' in loaded_data:
        datax = loaded_data['data']

        # Verificar si la clave 'partidos' está presente en la estructura de datos
        if 'partidos' in datax:
            extract = datax['partidos']

            # Crear DataFrame y realizar las operaciones necesarias
            e = pd.DataFrame.from_dict(extract, orient='index', columns=['value']).transpose().reset_index(drop=True)
            e['Date'] = loaded_data['status']['timestamp']
            e.to_csv(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv", index=False, mode='a')
        else:
            print("La clave 'partidos' no está presente en la estructura de datos.")
    else:
        print("La clave 'data' no está presente en la estructura de datos.")


def conexion_redshift(exec_date):
    print(f"Conectandose a la BD en la fecha: {exec_date}") 
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    try:
        conn = psycopg2.connect(
            host=url,
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port='5439')
        print(conn)
        print("Connected to Redshift successfully!")
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)

from psycopg2.extras import execute_values

import requests

def obtener_datos_de_api(exec_date, token):
    try:
        url = "https://api.football-data.org/v4/matches"
        response = requests.get(url, params={'date': exec_date}, headers={'X-Auth-Token': token})
        response.raise_for_status()  # Lanza una excepción si hay un código de estado de error
        data = response.json().get("matches", [])  # Accede al contenido JSON utilizando .json() y obtén la clave "matches"
        return data
    except Exception as e:
        print(f"Error al obtener datos de la API: {e}")
        raise




# Funcion de envio de data
def cargar_data(exec_date):
    try:
        print(f"Cargando la data para la fecha: {exec_date}")
        date = datetime.strptime(exec_date, '%Y-%m-%d %H')

        # Obtener datos desde la API
        data = obtener_datos_de_api(exec_date, token="08b983fb988b4db0a2ea9e21d1e15f51")

        # Conexión a la base de datos
        url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
        conn = psycopg2.connect(
            host=url,
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port='5439'
        )

        columns = [
            'utcDate',
            'competition',
            'status',
            'stage',
            'matchday',
            'homeTeam',
            'awayTeam',
            'referees',
            'score'
        ]

        from psycopg2.extras import execute_values
        cur = conn.cursor()

        # Define el nombre de la tabla
        table_name = 'partidos'

        # Define las columnas en las que deseas insertar datos
        columns = columns

        # Genera los valores
        values = [tuple(x) for x in data]
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"

        # Ejecuta la instrucción INSERT utilizando execute_values
        cur.execute("BEGIN")
        execute_values(cur, insert_sql, values)
        cur.execute("COMMIT")

        print("Datos insertados correctamente en la base de datos.")
    except Exception as e:
        print(f"Error en la carga de datos: {e}")


    ##1. Extraccion
task_1 = PythonOperator(
    task_id='extraer_data',
    python_callable=extraer_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=futbol_dag,
)

#2. Transformacion
task_2 = PythonOperator(
    task_id='transformar_data',
    python_callable=transformar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=futbol_dag,
)

# 3. Envio de data 
# 3.1 Conexion a base de datos
task_31= PythonOperator(
    task_id="conexion_BD",
    python_callable=conexion_redshift,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=futbol_dag,
)

# 3.2 Envio final
task_32 = PythonOperator(
    task_id='cargar_data',
    python_callable=cargar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=futbol_dag,
)

# Definicion orden de tareas
task_1 >> task_2 >> task_31 >> task_32