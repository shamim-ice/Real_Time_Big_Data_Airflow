from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, from_json
from pyspark.sql.types import StringType, DoubleType, StructType, StructField


spark=SparkSession.builder.appName('rtKafkaSpark')\
    .config('spark.sql.streaming.schemaInference','true')\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
    .config("spark.driver.extraClassPath", "/home/shamim/spark/jars/postgresql-42.3.8.jar")\
    .config("spark.executor.extraClassPath", "/home/shamim/spark/jars/postgresql-42.3.8.jar")\
    .getOrCreate()



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

def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/airflow_db") \
        .option("dbtable", "temperature_info") \
        .option("user", "postgres") \
        .option("password", "1418") \
        .mode("append") \
        .save()

parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start() \
    .awaitTermination()



