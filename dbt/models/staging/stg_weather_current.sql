{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT 
    city,
    country,
    latitude,
    longitude,
    temperature,
    feels_like,
    humidity,
    pressure,
    wind_speed,
    wind_direction,
    clouds,
    weather_description,
    icon,
    timestamp as observation_time,
    sunrise,
    sunset,
    timezone,
    data_source,
    ingested_at
FROM {{ source('weather_db', 'raw_weather') }}
WHERE timestamp >= DATEADD(day, -1, CURRENT_DATE())
QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY timestamp DESC) = 1