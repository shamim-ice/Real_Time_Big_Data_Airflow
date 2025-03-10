from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, from_json
from pyspark.sql.types import DateType,IntegerType, StringType, DoubleType, StructType, StructField,TimestampType


spark=SparkSession.builder.appName('RT_Data_Producer')\
    .config('spark.sql.streaming.schemaInference','true')\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
    .config("spark.driver.extraClassPath", "/home/shamim/spark/jars/postgresql-42.3.8.jar")\
    .config("spark.executor.extraClassPath", "/home/shamim/spark/jars/postgresql-42.3.8.jar")\
    .getOrCreate()



# Read stream from Kafka
df = spark.readStream\
    .format('kafka')\
    .option('kafka.bootstrap.servers', 'localhost:9092')\
    .option('subscribe', 'weather_data')\
    .option('startingOffsets', 'latest')\
    .load()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("date_time", TimestampType(), True),
    StructField('city', StringType(),True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField('pressure', DoubleType(),True),
    StructField('wind_speed', DoubleType(), True),
    StructField('wind_direction', StringType(), True),
    StructField('precipitation', StringType(), True),
    StructField('cloud_cover', DoubleType(), True)
    ])

parsed_df = df.selectExpr('CAST(value AS STRING)')\
    .select(from_json(col("value"), schema).alias("data"))\
    .select("data.*")

def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "weather_info") \
        .option("user", "postgres") \
        .option("password", "1418") \
        .mode("append") \
        .save()

parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start() \
    .awaitTermination()



