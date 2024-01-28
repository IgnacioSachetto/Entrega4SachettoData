#Importacion de Librerias
from datetime import datetime, timedelta
import requests
import psycopg2
import smtplib

import logging
import pandas as pd
from airflow import DAG
from credentials import FOOTBALL_API_TOKEN, HOST_REDSHIFT, USUARIO_REDSHIFT, CONTRASEÑA_REDSHIFT, PUERTO_REDSHIFT, BASEDEDATOS_REDSHIFT , EMAIL_DATA , EMAIL_PASS , EMAIL_SUBJECT_FAIL , EMAIL_SUBJECT_SUCESS , EMAIL_BODY_FAIL , EMAIL_BODY_SUCESS , EMAIL_DESTINY , TIMES_EMAIL_SENT
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from psycopg2.extras import execute_values

#Conexión a Redshift usando varibles de entorno (Utilizar archivo credentials.py)
redshift_conn = {
    'host': HOST_REDSHIFT,
    'username': USUARIO_REDSHIFT,
    'database': BASEDEDATOS_REDSHIFT,
    'port': '5439',
    'pwd': CONTRASEÑA_REDSHIFT
}

#Tomar el dia de hoy
current_datetime = datetime.utcnow()

default_args = {
    'owner': 'Ignacio Sachetto',
    'start_date': current_datetime,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
#Definición del DAG y sus atributos
futbol_dag = DAG(
    dag_id='ETLProcesoCompleto',
    default_args=default_args,
    description='Agrega datos de los partidos de las principales ligas del mundo diariamente.',
    schedule_interval="@daily", #Para una ejecución diaria.
    catchup=False # Evita la ejecución de tareas para fechas anteriores.
)

#1° Tarea = Verificar la conexión con RedShift, usando las credenciales previamente definidas.
def conexion_redshift(exec_date, **kwargs):
    logging.info(f"Conectandose a la BD en la fecha: {exec_date}")
    try:
        conn = psycopg2.connect(
            host=redshift_conn["host"],
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port='5439')
        logging.info("Conectado a RedShift Correctamente!")
    except Exception as e:
        logging.error("No se pudo conectar a Redshift :().")
        logging.error(e)


#2° Tarea = Verificar la conexión con la API, usando las credenciales previamente definidas.
def verificar_respuesta_api(exec_date, **kwargs):
    base_url = 'https://api.football-data.org/v4/'
    url_team_random = f'{base_url}teams/99'
    response_team_data = requests.get(url_team_random, headers={'X-Auth-Token': FOOTBALL_API_TOKEN })

    if response_team_data.status_code == 200:
        logging.info('Conexión correcta a la API')
    else:
        logging.error('Error al obtener los partidos')


#3° Tarea = Transformación y Extracción de Datos
def transformacion_extraccion_datos(exec_date,**kwargs):
    url_matches = 'https://api.football-data.org/v4/matches'
    competition_codes = ['PL', 'BL1', 'SA', 'PD', 'FL1'] #Defino las competiciones que me interesan almacenar
    dataframes = [] #Defino el Dataframe

    hoy = datetime.now() #Obtengo el día de hoy.

    anteayer = hoy - timedelta(days = 2)  # Para obtener el periodo (dia_actual - 2, dia_actual - 1). Decidí utilizar este periodo considerando posibles retrasos a la carga de datos de la API, se podría quitar la resta y obtener el periodo del dia anterior al actual.
    ayer = hoy - timedelta(days = 1) #Obtengo el día de ayer.

    anteayer_str = anteayer.strftime('%Y-%m-%d') #Le doy formato a la fecha para que tenga el mismo formato que los partidos cargados en la API.
    ayer_str = ayer.strftime('%Y-%m-%d') #Idem Anterior.

    logging.info(f'date_from: {anteayer}') #Para verificar que el formato sea correcto.
    logging.info(f'date_to: {ayer_str}') #Idem anterior.

    date_from = anteayer_str
    date_to = ayer_str


    #Obtención de los datos desde la API para el periodo indicado anteriormente.
    try:
        for code in competition_codes:
            # Para configurar los parámetros para la solicitud a la API.
            params = {
                'dateFrom': date_from,
                'dateTo': date_to,
                'competitions': code
            }

            # Para realizar la solicitud a la API con los parámetros y el token de autenticación.
            response = requests.get(url_matches, params=params, headers={'X-Auth-Token': FOOTBALL_API_TOKEN})

            # Si la API responde con un código 200 (OK), proceso los datos.
            if response.status_code == 200:
                data = response.json()
                # Verifico si hay partidos en la respuesta de la API.
                if 'matches' not in data:
                    print(f"No hay partidos disponibles para la competición: {params['competitions']}")
                    continue
                else:
                    # Si hay partidos creo un DataFrame con los datos de los partidos.
                    df = pd.DataFrame(data['matches'])
                    # Verifico que el dataframe se haya creado correctamente
                    if df.empty:
                        logging.info("No hay datos de partidos disponibles.")
                        continue
                    else:
                    # Proceso y agrego las columnas al DataFrame basandome el los atributos que me interesan brindados por la API.

                        df['fecha_partido'] = df['utcDate']
                        df['competicion'] = df['competition'].apply(lambda x: x['name'])
                        df['estado'] = df['status']
                        df['etapa'] = df['stage']
                        df['jornada'] = df['matchday']
                        df['equipo_local'] = df['homeTeam'].apply(lambda x: x['name'])
                        df['equipo_visitante'] = df['awayTeam'].apply(lambda x: x['name'])
                        df['arbitro'] = df['referees'].apply(lambda x: x[0]['name'] if x else None)
                        df['resultado'] = df['score'].apply(lambda x: f"{x['fullTime']['home']} - {x['fullTime']['away']}" if x['fullTime'] else None)
                    #Muestro la cantidad de partidos obtenidos para cada liga y agrego el df al listado de dataframes.
                        logging.info(f'Data obtenida: {df.shape[0]} partidos recuperados.')
                        dataframes.append(df)
                    # Convierto los datos  a un formato adecuado para almacenar en XCom utilizado por AirFlow para compartir datos entre las tareas.
                        combined_df = pd.concat(dataframes, ignore_index=True)

                        xcom_value = combined_df.to_dict(orient='records')
                        return xcom_value #Devuelvo los datos del  xcom para utilizarlo en la proxima tarea de carga
    except Exception as e:
        logging.error(f'Error en la extracción de datos: {e}')
        raise e

#4° Tarea con todo procesado y la conexión verificada cargo los datos a Redshift
def cargar_datos_redshift(exec_date, **kwargs):
    #Vuelvo a conectarme a RedShfit con los datos que verifique anteriormente que funcionan.

    num_intento = 1
    registros_insertados = 0

    conn = psycopg2.connect(
        host=redshift_conn["host"],
        dbname=redshift_conn["database"],
        user=redshift_conn["username"],
        password=redshift_conn["pwd"],
        port='5439')

    # Obtengo xcom_value desde la tarea anterior para poder utilizarlo en la tarea de carga.
    xcom_value = kwargs.get('task_instance').xcom_pull(task_ids='transformacion_extraccion_datos')
    #Verifico que xcom no este vacio.
    if xcom_value is None or not xcom_value:
        logging.info(f'Sin partidos nuevos.')
        return

    cur = conn.cursor()

    #Comienzo con la carga de datos, definiendo la tabla que previamente cree en RedShift y los nombres de cada columna
    try:
        table_name = 'Partidos'
        columns = ['fecha_partido', 'competicion', 'estado',
                'etapa', 'jornada', 'equipo_local',
                'equipo_visitante', 'arbitro', 'resultado']
    #Verifico que los datos que voy a cargar no estén previamente cargados en RedShift
        for row in xcom_value:
            fecha_partido = row['fecha_partido']
            competicion = row['competicion']
            equipo_local = row['equipo_local']
            equipo_visitante = row['equipo_visitante']

            cur.execute("""
                            SELECT COUNT(*) FROM Partidos
                            WHERE fecha_partido = %s AND competicion = %s AND equipo_local = %s AND equipo_visitante = %s
                        """, (
                            fecha_partido, competicion, equipo_local, equipo_visitante
                        ))

            count = cur.fetchone()[0]
            logging.info(f'count: {count}')
            #Si no hay registros duplicados cargo los datos a RedShift
            if count == 0:
                values = [tuple(row[column] for column in columns) for row in xcom_value]

                insert_sql = 'INSERT INTO "{}" ({}) VALUES %s'.format(table_name, ', '.join('"' + col + '"' for col in columns))

                cur.execute("BEGIN")
                execute_values(cur, insert_sql, values)
                cur.execute("COMMIT")

                registros_insertados += len(values)


                logging.info("Datos cargados correctamente en Redshift para el registro actual.")

                enviar_email(EMAIL_SUBJECT_SUCESS, EMAIL_BODY_SUCESS, EMAIL_DESTINY,num_intento,registros_insertados)
                num_intento = 0


            else:
                logging.warning("Los datos ya existen en la base de datos para el registro actual. No se realizará la inserción.")

    except Exception as e:
        logging.error(f"Error al intentar cargar datos en Redshift: {str(e)}")
        if num_intento < TIMES_EMAIL_SENT:
            enviar_email(EMAIL_SUBJECT_FAIL, EMAIL_BODY_FAIL, EMAIL_DESTINY,num_intento,registros_insertados)

    #Cierro la conexión
    finally:
        cur.close()
        conn.close()

        logging.info("Finalizado")


def enviar_email(subject, body_text, to_email, num_intento,registros_insertados):
    user = EMAIL_DATA
    pwd_email = EMAIL_PASS

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(user, pwd_email)

            # Agrega el número de intento al cuerpo del correo electrónico
            message = f'Subject: {subject}\n\n{body_text}\nFecha y hora de ejecución: {datetime.now()}.\nCantidad de Registros Nuevos Insertados: {registros_insertados}.\nEste fue el intento de carga N°: {num_intento}.'.encode('utf-8')
            server.sendmail(user, to_email, message)


        # Agrega la hora de ejecución al mensaje de registro de éxito
        success_message = f'Email enviado con éxito a las {datetime.now()}'
        logging.info(success_message)

    except Exception as exception:
        # Agrega la hora de ejecución al mensaje de registro de error
        error_message = f'Falló el envío del email a las {datetime.now()}: {str(exception)}'
        logging.error(error_message)




#Defino cada una de las tareas secuenciales del DAG
conexion_redshift = PythonOperator(
    task_id="conexion_BD",
    python_callable=conexion_redshift,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    provide_context=True,
    dag=futbol_dag,
)

conexion_api = PythonOperator(
    task_id='respuesta_api',
    python_callable=verificar_respuesta_api,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    provide_context=True,
    dag=futbol_dag,
)


extraccion_transformacion = PythonOperator(
    task_id='transformacion_extraccion_datos',
    python_callable=transformacion_extraccion_datos,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    provide_context=True,
    dag=futbol_dag,
)


carga_redshift = PythonOperator(
    task_id='cargar_redshift_task',
    python_callable=cargar_datos_redshift,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    provide_context=True,
    dag=futbol_dag,
)

#Defino el flujo del DAG.
conexion_redshift >> conexion_api >> extraccion_transformacion >> carga_redshift

#cc