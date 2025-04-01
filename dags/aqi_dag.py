from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import json

# กำหนดค่าเริ่มต้นของ DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ฟังก์ชันดึงข้อมูลจาก AirVisual API
def fetch_aqi():
    API_URL = "https://api.airvisual.com/v2/city?city=Bangkok&state=Bangkok&country=Thailand&key=YOUR_API_KEY"
    response = requests.get(API_URL)
    data = response.json()
    
    # ดึงเฉพาะข้อมูลที่ต้องการ
    aqi = data['data']['current']['pollution']['aqius']
    timestamp = data['data']['current']['pollution']['ts']

    # บันทึกลง PostgreSQL
    conn = psycopg2.connect(dbname="airquality", user="postgres", password="mysecretpassword", host="localhost")
    cur = conn.cursor()
    cur.execute("INSERT INTO aqi_data (timestamp, aqi) VALUES (%s, %s)", (timestamp, aqi))
    conn.commit()
    cur.close()
    conn.close()

# สร้าง DAG
dag = DAG('aqi_pipeline', default_args=default_args, schedule_interval='@daily')

# สร้าง Task
fetch_aqi_task = PythonOperator(
    task_id='fetch_aqi_data',
    python_callable=fetch_aqi,
    dag=dag,
)

fetch_aqi_task
