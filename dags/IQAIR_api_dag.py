import json
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

import requests

DAG_FOLDER = "/opt/airflow/dags"
API_KEY = "eaba6b84-ec0f-4a86-8e93-1fa9851fd676"
CITY = "Bangkok"
STATE = "Bangkok"
COUNTRY = "Thailand"

def _get_air_quality_data():
    url = "http://api.airvisual.com/v2/city?city={city}&state={state}&country={country}&key={API_KEY}"
    params = {
        "city": CITY,
        "state": STATE,
        "country": COUNTRY,
        "key": API_KEY,
    }
    response = requests.get(url, params=params)
    print(response.url)

    data = response.json()
    print(data)

    with open(f"{DAG_FOLDER}/air_quality.json", "w") as f:
        json.dump(data, f)

def _validate_data():
    with open(f"{DAG_FOLDER}/air_quality.json", "r") as f:
        data = json.load(f)
    
    assert data.get("data") is not None

def _create_air_quality_table():
    pg_hook = PostgresHook(
        postgres_conn_id="weather_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        CREATE TABLE IF NOT EXISTS air_quality (
            ts TIMESTAMP NOT NULL,
            aqi_us INT NOT NULL,
            aqi_cn INT NOT NULL,
            temperature FLOAT NOT NULL,
            humidity INT NOT NULL
        )
    """
    cursor.execute(sql)
    connection.commit()

def _load_data_to_postgres():
    pg_hook = PostgresHook(
        postgres_conn_id="weather_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    with open(f"{DAG_FOLDER}/air_quality.json", "r") as f:
        data = json.load(f)
    
    ts = data["data"]["current"]["pollution"]["ts"]
    aqi_us = data["data"]["current"]["pollution"]["aqius"]
    aqi_cn = data["data"]["current"]["pollution"]["aqicn"]
    temperature = data["data"]["current"]["weather"]["tp"]
    humidity = data["data"]["current"]["weather"]["hu"]

    sql = f"""
        INSERT INTO air_quality (ts, aqi_us, aqi_cn, temperature, humidity)
        VALUES ('{ts}', {aqi_us}, {aqi_cn}, {temperature}, {humidity})
    """
    cursor.execute(sql)
    connection.commit()

default_args = {
    "email": ["kan@odds.team"],
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}
with DAG(
    "air_quality_dag",
    default_args=default_args,
    schedule="0 */3 * * *",
    start_date=timezone.datetime(2025, 3, 1),
    tags=["air_quality"],
):
    start = EmptyOperator(task_id="start")

    get_air_quality_data = PythonOperator(
        task_id="get_air_quality_data",
        python_callable=_get_air_quality_data,
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_data,
    )

    create_air_quality_table = PythonOperator(
        task_id="create_air_quality_table",
        python_callable=_create_air_quality_table,
    )

    load_data_to_postgres = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=_load_data_to_postgres,
    )

    end = EmptyOperator(task_id="end")

    start >> get_air_quality_data >> validate_data >> load_data_to_postgres >> end
    start >> create_air_quality_table >> load_data_to_postgres
