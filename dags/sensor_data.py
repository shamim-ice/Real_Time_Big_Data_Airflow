from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, from_json
from pyspark.sql.types import StringType, DoubleType, StructType, StructField
import pandas as pd
import psycopg2
import json
from kafka import KafkaConsumer


spark=SparkSession.builder.appName('rtKafkaSpark')\
    .config('spark.sql.streaming.schemaInference','true')\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
    .getOrCreate()


#print(spark.sparkContext.getConf().get("spark.jars"))

# Read stream from Kafka
df = spark.readStream.format('kafka')\
    .option('kafka.bootstrap.servers', 'localhost:9092')\
    .option('subscribe', 'sensor_data')\
    .option('startingOffsets', 'latest')\
    .load()

schema = StructType([
    StructField("id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True)
    ])

parsed_df = df.selectExpr('CAST(value AS STRING)')\
    .select(from_json(col("value"), schema).alias("data"))\
    .select("data.*")

query = parsed_df.writeStream.outputMode('append')\
    .format('console')\
    .start()

query.awaitTermination()

#Establish connection to postgreSQL database
conn = psycopg2.connect(
    database='airflow_db', # databse name
    user='postgres',      #databse user
    password='1418',   #password
    host='localhost',
    port='5432'
)

cur = conn.cursor()

# Kafka Consumer
consumer = KafkaConsumer('sensor_data', bootstrap_servers='localhost:9092')

for msg in parsed_df:
    data = json.loads(msg.value)
    cur.execute("INSERT INTO temperature_info (id, date, temperature, humidity) \
                VALUES (%s, %s, %s, %s)", \
                    (data['id'], data['date'], data['temperature'], data['humidity']))
    conn.commit()

#spark.stop()
