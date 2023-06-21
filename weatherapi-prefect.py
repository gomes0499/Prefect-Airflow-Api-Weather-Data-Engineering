from prefect import task, Flow
from google.cloud import bigquery
import requests
import pandas as pd

# Inicializando o cliente do BigQuery
project_id = 'airflowweather'
client = bigquery.Client(project=project_id)

@task
def extract():
    response = requests.get("http://api.openweathermap.org/data/2.5/weather?q=London&appid=7ecafd96219b7af2ea080fcf8c505e1d")
    data = response.json()
    return data

@task
def transform(data):
    temp = data['main']['temp']
    humidity = data['main']['humidity']
    name = data['name']
    data_df = pd.DataFrame([{'name': name, 'temp': temp, 'humidity': humidity}])
    return data_df.to_json()

@task
def load(data_df):
    data_df = pd.read_json(data_df)
    table_id = "airflowweather.weatherdataset.weathertable"
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("temp", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("humidity", bigquery.enums.SqlTypeNames.FLOAT64),
    ])
    job = client.load_table_from_dataframe(data_df, table_id, job_config=job_config)
    job.result()  # Aguarde a conclusão do trabalho
    if job.errors:
        raise Exception(f'BigQuery job failed: {job.errors}')

@Flow
def weather_etl():
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

