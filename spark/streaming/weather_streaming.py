from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Схема данных
weather_schema = StructType([
    StructField("city", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", IntegerType()),
    StructField("pressure", IntegerType()),
    StructField("wind_speed", DoubleType()),
    StructField("timestamp", TimestampType())
])

def main():
    # Создание Spark сессии
    spark = SparkSession.builder \
        .appName("WeatherStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()
    
    # Чтение из Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "weather-data") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Парсинг JSON
    parsed_df = df.select(
        from_json(col("value").cast("string"), weather_schema).alias("data")
    ).select("data.*")
    
    # Агрегации в реальном времени
    aggregated_df = parsed_df \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy("city") \
        .agg(
            {"temperature": "avg", "humidity": "avg", "wind_speed": "avg"}
        )
    
    # Запись в ClickHouse
    def write_to_clickhouse(batch_df, batch_id):
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:clickhouse://clickhouse:8123/weather_db") \
            .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
            .option("dbtable", "weather_realtime_agg") \
            .option("user", "spark") \
            .option("password", "spark_pass") \
            .mode("append") \
            .save()
    
    query = aggregated_df.writeStream \
        .foreachBatch(write_to_clickhouse) \
        .outputMode("update") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()