from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import os
import shutil
import requests

default_args = {
    'owner': 'airflow_user',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
}

CARPETA_ORIGEN = r"C:\Users\Agustin Cordoba\Desktop\proyecto\origen"
CARPETA_PROCESADOS = r"C:\Users\Agustin Cordoba\Desktop\proyecto\procesados"

with DAG(
    dag_id="etl_spaceX",
    default_args=default_args,
    description="Monitorea archivos Excel y datos de API para exportar a una base de datos.",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 30),
    catchup=False,
    max_active_runs=1,
) as dag:

    def obtener_y_guardar_api():
        url = "https://api.spacexdata.com/v4/launches/past"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)  

            columnas_interes = ["name", "date_utc", "success", "rocket"]
            df = df[columnas_interes]

            archivo_excel = os.path.join(CARPETA_ORIGEN, "spacex_data.xlsx")
            df.to_excel(archivo_excel, index=False)
            print(f"Datos de la API guardados en: {archivo_excel}")
        else:
            raise ValueError("No se pudo obtener datos de la API")

    obtener_datos_api = PythonOperator(
        task_id="obtener_datos_api",
        python_callable=obtener_y_guardar_api
    )

    sensor_archivo = FileSensor(
        task_id="monitorear_archivo_xlsx",
        filepath=os.path.join(CARPETA_ORIGEN, "*.xlsx"),  
        poke_interval=30,  
        timeout=600,  
        mode="poke"
    )

    def procesar_excel(**kwargs):
        archivos = [f for f in os.listdir(CARPETA_ORIGEN) if f.endswith(".xlsx")]
        if not archivos:
            raise ValueError("No se encontraron archivos Excel.")

        for archivo in archivos:
            ruta_archivo = os.path.join(CARPETA_ORIGEN, archivo)
            df = pd.read_excel(ruta_archivo)

            print(f"Datos de {archivo} cargados a la base de datos")

            shutil.move(ruta_archivo, os.path.join(CARPETA_PROCESADOS, archivo))
            print(f"Archivo {archivo} movido a {CARPETA_PROCESADOS}")

    procesar_archivo = PythonOperator(
        task_id="procesar_archivo",
        python_callable=procesar_excel,
    )

    def push_xcom(**kwargs):
        kwargs['ti'].xcom_push(key='mensaje', value='Archivos procesados correctamente')

    xcom_push = PythonOperator(
        task_id='guardar_xcom',
        python_callable=push_xcom
    )
   
    def pull_xcom(**kwargs):
        mensaje = kwargs['ti'].xcom_pull(key='mensaje', task_ids='guardar_xcom')
        print(f"Mensaje recibido desde XCom: {mensaje}")

    xcom_pull = PythonOperator(
        task_id='recuperar_xcom',
        python_callable=pull_xcom
    )

    def evaluar_condicion(**kwargs):
        archivos = os.listdir(CARPETA_PROCESADOS)
        return "enviar_correo" if archivos else "no_hay_datos"

    branching = BranchPythonOperator(
        task_id="evaluar_condicion",
        python_callable=evaluar_condicion,
    )

    email = EmailOperator(
        task_id='enviar_correo',
        to=['analyst@empresa.com'],
        subject="Datos Procesados",
        html_content="Los datos han sido cargados correctamente en la base de datos.",
    )

    no_hay_datos = BashOperator(
        task_id="no_hay_datos",
        bash_command="echo 'No hay datos procesados hoy.'"
    )
    
    obtener_datos_api >> sensor_archivo >> procesar_archivo >> [xcom_push, xcom_pull]
    xcom_pull >> branching
    branching >> [email, no_hay_datos]
