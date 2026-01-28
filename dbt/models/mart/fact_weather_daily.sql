{{ config(
    materialized='table',
    schema='mart',
    tags=['daily', 'aggregates']
) }}

WITH daily_weather AS (
    SELECT 
        city,
        DATE(timestamp) as date,
        AVG(temperature) as avg_temp,
        MIN(temperature) as min_temp,
        MAX(temperature) as max_temp,
        AVG(humidity) as avg_humidity,
        AVG(pressure) as avg_pressure,
        AVG(wind_speed) as avg_wind_speed,
        MAX(wind_speed) as max_wind_speed,
        ANY(weather_description) as dominant_condition,
        COUNT(*) as observations_count
    FROM {{ ref('stg_weather_current') }}
    GROUP BY city, DATE(timestamp)
)
SELECT 
    *,
    CASE 
        WHEN avg_temp < 0 THEN 'Freezing'
        WHEN avg_temp < 10 THEN 'Cold'
        WHEN avg_temp < 20 THEN 'Cool'
        WHEN avg_temp < 30 THEN 'Warm'
        ELSE 'Hot'
    END as temperature_category,
    CASE 
        WHEN avg_humidity < 30 THEN 'Dry'
        WHEN avg_humidity < 60 THEN 'Comfortable'
        ELSE 'Humid'
    END as humidity_category
FROM daily_weather