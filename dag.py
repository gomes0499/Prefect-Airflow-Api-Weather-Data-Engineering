# Importando bibliotecas necessárias
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.dummy_operator import DummyOperator
from google.cloud import bigquery
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import pandas as pd

# Inicializando o cliente do BigQuery
client = bigquery.Client()

# Definindo argumentos padrões
default_args = {
    'owner': 'gomes',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['notify.data.pipeline@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Inicializando o DAG
dag = DAG(
    'weather_etl',
    default_args=default_args,
    description='ETL pipeline for weather data using API',
    schedule_interval=timedelta(days=1),
)

# Função para extração dos dados
def extract(**kwargs):
    ti = kwargs['ti']
    response = requests.get("http://api.openweathermap.org/data/2.5/weather?q=London&appid=7ecafd96219b7af2ea080fcf8c505e1d")
    data = response.json()
    ti.xcom_push(key='data', value=data)

# Função para transformação dos dados
def transform(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='data', task_ids='extract')
    data_df = pd.json_normalize(data)
    data_df = data_df[['name', 'main.temp', 'main.humidity']]
    ti.xcom_push(key='data_df', value=data_df.to_json())

# Função para carga dos dados
def load(**kwargs):
    ti = kwargs['ti']
    data_df = pd.read_json(ti.xcom_pull(key='data_df', task_ids='transform'))

    # Converta o DataFrame em um objeto do BigQuery
    table_id = "airflowweather.weatherdataset.weathertable"
    job_config = bigquery.LoadJobConfig(schema=[
        # Defina a estrutura da tabela conforme necessário
        bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("temp", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("humidity", bigquery.enums.SqlTypeNames.FLOAT64),
    ])

    job = client.load_table_from_dataframe(data_df, table_id, job_config=job_config)
    job.result()  # Aguarde a conclusão do trabalho

    if job.errors:
        raise Exception(f'BigQuery job failed: {job.errors}')

# Definindo os operadores do DAG
start = DummyOperator(task_id='start', dag=dag)
extract = PythonOperator(task_id='extract', python_callable=extract, provide_context=True, dag=dag)
transform = PythonOperator(task_id='transform', python_callable=transform, provide_context=True, dag=dag)
load = PythonOperator(task_id='load', python_callable=load, provide_context=True, dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Definindo a sequência do DAG
start >> extract >> transform >> load >> end