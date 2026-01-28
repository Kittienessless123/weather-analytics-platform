from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from datetime import date, datetime, timedelta
import pandas as pd
from clickhouse_driver import Client
from pydantic import BaseModel

app = FastAPI(
    title="Weather Analytics API",
    description="API для доступа к данным о погоде",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Модели Pydantic
class WeatherCurrent(BaseModel):
    city: str
    temperature: float
    feels_like: float
    humidity: int
    pressure: int
    wind_speed: float
    weather_description: str
    observation_time: datetime
    sunrise: datetime
    sunset: datetime

class WeatherHistory(BaseModel):
    date: date
    avg_temp: float
    min_temp: float
    max_temp: float
    avg_humidity: float
    total_precipitation: float

class CityStats(BaseModel):
    city: str
    days_analyzed: int
    avg_temperature: float
    coldest_day: date
    warmest_day: date
    rainy_days: int

# Зависимость для ClickHouse
def get_clickhouse_client():
    client = Client(
        host='clickhouse',
        port=9000,
        user='api',
        password='api_pass',
        database='weather_db'
    )
    try:
        yield client
    finally:
        client.disconnect()

# Роуты
@app.get("/")
async def root():
    return {
        "message": "Weather Analytics API",
        "version": "1.0.0",
        "endpoints": {
            "current": "/api/v1/weather/current",
            "history": "/api/v1/weather/history/{city}",
            "cities": "/api/v1/cities",
            "stats": "/api/v1/stats/{city}"
        }
    }

@app.get("/api/v1/weather/current", response_model=List[WeatherCurrent])
async def get_current_weather(
    client: Client = Depends(get_clickhouse_client),
    city: Optional[str] = None
):
    """Получить текущую погоду для всех или конкретного города"""
    
    if city:
        query = """
        SELECT 
            city,
            temperature,
            feels_like,
            humidity,
            pressure,
            wind_speed,
            weather_description,
            timestamp as observation_time,
            sunrise,
            sunset
        FROM raw_weather
        WHERE city = %(city)s
        ORDER BY timestamp DESC
        LIMIT 1
        """
        params = {'city': city}
    else:
        query = """
        SELECT 
            city,
            argMax(temperature, timestamp) as temperature,
            argMax(feels_like, timestamp) as feels_like,
            argMax(humidity, timestamp) as humidity,
            argMax(pressure, timestamp) as pressure,
            argMax(wind_speed, timestamp) as wind_speed,
            argMax(weather_description, timestamp) as weather_description,
            max(timestamp) as observation_time,
            argMax(sunrise, timestamp) as sunrise,
            argMax(sunset, timestamp) as sunset
        FROM raw_weather
        WHERE timestamp > now() - INTERVAL 1 HOUR
        GROUP BY city
        """
        params = {}
    
    result = client.execute(query, params)
    
    return [
        WeatherCurrent(
            city=row[0],
            temperature=row[1],
            feels_like=row[2],
            humidity=row[3],
            pressure=row[4],
            wind_speed=row[5],
            weather_description=row[6],
            observation_time=row[7],
            sunrise=row[8],
            sunset=row[9]
        )
        for row in result
    ]

@app.get("/api/v1/weather/history/{city}", response_model=List[WeatherHistory])
async def get_weather_history(
    city: str,
    start_date: date = Query(default=date.today() - timedelta(days=30)),
    end_date: date = Query(default=date.today()),
    client: Client = Depends(get_clickhouse_client)
):
    """Получить историю погоды для города"""
    
    query = """
    SELECT 
        DATE(timestamp) as date,
        AVG(temperature) as avg_temp,
        MIN(temperature) as min_temp,
        MAX(temperature) as max_temp,
        AVG(humidity) as avg_humidity,
        0 as total_precipitation  -- Заглушка, если нет данных об осадках
    FROM raw_weather
    WHERE city = %(city)s
      AND DATE(timestamp) BETWEEN %(start_date)s AND %(end_date)s
    GROUP BY DATE(timestamp)
    ORDER BY date
    """
    
    result = client.execute(query, {
        'city': city,
        'start_date': start_date,
        'end_date': end_date
    })
    
    return [
        WeatherHistory(
            date=row[0],
            avg_temp=row[1],
            min_temp=row[2],
            max_temp=row[3],
            avg_humidity=row[4],
            total_precipitation=row[5]
        )
        for row in result
    ]

@app.get("/api/v1/cities")
async def get_available_cities(client: Client = Depends(get_clickhouse_client)):
    """Получить список доступных городов"""
    
    query = "SELECT DISTINCT city FROM raw_weather ORDER BY city"
    result = client.execute(query)
    
    return {"cities": [row[0] for row in result]}

@app.get("/api/v1/stats/{city}", response_model=CityStats)
async def get_city_stats(city: str, client: Client = Depends(get_clickhouse_client)):
    """Получить статистику по городу"""
    
    query = """
    SELECT 
        COUNT(DISTINCT DATE(timestamp)) as days_count,
        AVG(temperature) as avg_temp,
        MIN(temperature) as min_temp,
        MAX(temperature) as max_temp,
        SUM(CASE WHEN weather_description LIKE '%rain%' THEN 1 ELSE 0 END) as rainy_days,
        argMin(DATE(timestamp), temperature) as coldest_day,
        argMax(DATE(timestamp), temperature) as warmest_day
    FROM raw_weather
    WHERE city = %(city)s
      AND timestamp > now() - INTERVAL 30 DAY
    """
    
    result = client.execute(query, {'city': city})
    
    if not result:
        raise HTTPException(status_code=404, detail="City not found")
    
    row = result[0]
    
    return CityStats(
        city=city,
        days_analyzed=row[0],
        avg_temperature=round(row[1], 2),
        coldest_day=row[5],
        warmest_day=row[6],
        rainy_days=row[4]
    )

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.utcnow()}