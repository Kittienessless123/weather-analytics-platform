from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
import json
from typing import Dict, List

default_args = {
    'owner': 'weather_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

def fetch_weather_data(**context) -> List[Dict]:
    """Получение данных о погоде из OpenWeatherMap API"""
    cities = [
        ('Moscow', 'RU'),
        ('Saint Petersburg', 'RU'),
        ('Novosibirsk', 'RU'),
        ('Yekaterinburg', 'RU'),
        ('Kazan', 'RU'),
    ]
    
    API_KEY = context['var']['value'].get('OPENWEATHER_API_KEY')
    BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
    
    weather_data = []
    
    for city, country in cities:
        try:
            params = {
                'q': f'{city},{country}',
                'appid': API_KEY,
                'units': 'metric',
                'lang': 'ru'
            }
            
            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            weather_record = {
                'city': city,
                'country': country,
                'latitude': data['coord']['lat'],
                'longitude': data['coord']['lon'],
                'temperature': data['main']['temp'],
                'feels_like': data['main']['feels_like'],
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'wind_speed': data['wind']['speed'],
                'wind_direction': data['wind'].get('deg', 0),
                'clouds': data['clouds']['all'],
                'weather_description': data['weather'][0]['description'],
                'icon': data['weather'][0]['icon'],
                'timestamp': datetime.utcfromtimestamp(data['dt']).isoformat(),
                'sunrise': datetime.utcfromtimestamp(data['sys']['sunrise']).isoformat(),
                'sunset': datetime.utcfromtimestamp(data['sys']['sunset']).isoformat(),
                'timezone': data['timezone'],
                'data_source': 'openweathermap'
            }
            
            weather_data.append(weather_record)
            
        except Exception as e:
            print(f"Error fetching data for {city}: {e}")
            continue
    
    return weather_data

def transform_data(**context):
    """Трансформация данных"""
    ti = context['ti']
    raw_data = ti.xcom_pull(task_ids='fetch_weather_data')
    
    df = pd.DataFrame(raw_data)
    
    # Преобразование типов
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['sunrise'] = pd.to_datetime(df['sunrise'])
    df['sunset'] = pd.to_datetime(df['sunset'])
    
    # Добавление вычисляемых полей
    df['temperature_f'] = df['temperature'] * 9/5 + 32
    df['is_daytime'] = (df['timestamp'] > df['sunrise']) & (df['timestamp'] < df['sunset'])
    
    # Категоризация температуры
    def categorize_temp(temp):
        if temp < 0:
            return 'freezing'
        elif temp < 10:
            return 'cold'
        elif temp < 20:
            return 'cool'
        elif temp < 30:
            return 'warm'
        else:
            return 'hot'
    
    df['temp_category'] = df['temperature'].apply(categorize_temp)
    
    return df.to_dict('records')

def load_to_clickhouse(**context):
    """Загрузка данных в ClickHouse"""
    from clickhouse_driver import Client
    
    ti = context['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data')
    
    client = Client(
        host='clickhouse',
        port=9000,
        user='airflow',
        password='airflow_pass',
        database='weather_db'
    )
    
    # Вставка данных
    query = """
    INSERT INTO raw_weather (
        city, country, latitude, longitude, temperature, feels_like,
        humidity, pressure, wind_speed, wind_direction, clouds,
        weather_description, icon, timestamp, sunrise, sunset,
        timezone, data_source
    ) VALUES
    """
    
    values = []
    for record in transformed_data:
        values.append((
            record['city'],
            record['country'],
            record['latitude'],
            record['longitude'],
            record['temperature'],
            record['feels_like'],
            record['humidity'],
            record['pressure'],
            record['wind_speed'],
            record['wind_direction'],
            record['clouds'],
            record['weather_description'],
            record['icon'],
            record['timestamp'],
            record['sunrise'],
            record['sunset'],
            record['timezone'],
            record['data_source']
        ))
    
    client.execute(query, values)
    
    return f"Inserted {len(values)} records"

def run_data_quality_checks(**context):
    """Проверка качества данных"""
    from great_expectations.core import ExpectationSuite
    import great_expectations as ge
    
    ti = context['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data')
    
    df = pd.DataFrame(transformed_data)
    
    # Создание тестов
    suite = ExpectationSuite(expectation_suite_name="weather_data_suite")
    
    # Проверки
    suite.expect_column_values_to_not_be_null("city")
    suite.expect_column_values_to_be_between("temperature", -50, 50)
    suite.expect_column_values_to_be_between("humidity", 0, 100)
    
    results = df.validate(expectations=suite)
    
    if not results.success:
        raise ValueError(f"Data quality checks failed: {results}")
    
    return "Data quality checks passed"

with DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for weather data',
    schedule_interval='0 * * * *',  # Каждый час
    catchup=False,
    tags=['weather', 'etl', 'analytics'],
) as dag:
    
    # Task 1: Проверка доступности API
    check_api = HttpSensor(
        task_id='check_weather_api',
        http_conn_id='openweathermap_api',
        endpoint='/data/2.5/weather',
        request_params={'q': 'Moscow,RU', 'appid': '{{ var.value.OPENWEATHER_API_KEY }}'},
        mode='poke',
        timeout=300,
        poke_interval=60,
    )
    
    # Task 2: Получение данных
    fetch_data = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
        provide_context=True,
    )
    
    # Task 3: Трансформация
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )
    
    # Task 4: Проверка качества
    data_quality = PythonOperator(
        task_id='run_data_quality_checks',
        python_callable=run_data_quality_checks,
        provide_context=True,
    )
    
    # Task 5: Загрузка
    load_data = PythonOperator(
        task_id='load_to_clickhouse',
        python_callable=load_to_clickhouse,
        provide_context=True,
    )
    
    # Task 6: Обновление агрегатов
    update_aggregates = PythonOperator(
        task_id='update_aggregates',
        python_callable=lambda: "Aggregates updated",
        provide_context=True,
    )
    
    # Определение порядка выполнения
    check_api >> fetch_data >> transform >> data_quality >> load_data >> update_aggregates