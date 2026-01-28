-- Создание базы данных
CREATE DATABASE IF NOT EXISTS weather_db
ENGINE = Atomic;

USE weather_db;

-- Сырые данные из API
CREATE TABLE IF NOT EXISTS raw_weather (
    city String,
    country String,
    latitude Float64,
    longitude Float64,
    temperature Float64,
    feels_like Float64,
    humidity UInt8,
    pressure UInt16,
    wind_speed Float64,
    wind_direction UInt16,
    clouds UInt8,
    weather_description String,
    icon String,
    timestamp DateTime64(3, 'UTC'),
    sunrise DateTime64(3, 'UTC'),
    sunset DateTime64(3, 'UTC'),
    timezone Int32,
    data_source String DEFAULT 'openweathermap',
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (city, timestamp)
PARTITION BY toYYYYMM(timestamp)
TTL timestamp + INTERVAL 2 YEAR;

-- Исторические данные
CREATE TABLE IF NOT EXISTS weather_history (
    city String,
    date Date,
    avg_temp Float64,
    min_temp Float64,
    max_temp Float64,
    total_precipitation Float64,
    avg_humidity Float64,
    avg_wind_speed Float64,
    sunrise_time DateTime,
    sunset_time DateTime,
    weather_conditions String,
    days_with_rain UInt8,
    days_with_snow UInt8,
    days_with_storm UInt8,
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (city, date)
PARTITION BY toYYYY(date);

-- Агрегаты для быстрых отчетов
CREATE TABLE IF NOT EXISTS weather_daily_agg (
    city String,
    date Date,
    hour UInt8,
    avg_temperature Float64,
    avg_humidity Float64,
    total_precipitation Float64,
    wind_speed_avg Float64,
    weather_conditions Array(String),
    record_count UInt32,
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (city, date, hour);

-- Материализованное представление
CREATE MATERIALIZED VIEW weather_current_mv
TO weather_current
AS
SELECT 
    city,
    argMax(temperature, timestamp) as current_temp,
    argMax(humidity, timestamp) as current_humidity,
    argMax(pressure, timestamp) as current_pressure,
    argMax(wind_speed, timestamp) as current_wind_speed,
    max(timestamp) as last_updated
FROM raw_weather
WHERE timestamp > now() - INTERVAL 1 HOUR
GROUP BY city;

-- Создание пользователей
CREATE USER IF NOT EXISTS airflow IDENTIFIED WITH plaintext_password BY 'airflow_pass';
CREATE USER IF NOT EXISTS spark IDENTIFIED WITH plaintext_password BY 'spark_pass';
CREATE USER IF NOT EXISTS api IDENTIFIED WITH plaintext_password BY 'api_pass';

GRANT ALL ON weather_db.* TO airflow;
GRANT INSERT, SELECT ON weather_db.* TO spark;
GRANT SELECT ON weather_db.* TO api;