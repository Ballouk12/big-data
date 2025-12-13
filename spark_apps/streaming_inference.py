from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_json
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, IntegerType
from pyspark.ml import PipelineModel

# CONFIG
MODEL_PATH = "hdfs://namenode-bd:9000/models/weather_v1"

spark = SparkSession.builder \
    .appName("StreamingInference") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()

# LOAD MODEL
try:
    model = PipelineModel.load(MODEL_PATH)
    print("Model loaded successfully.")
except Exception as e:
    print("Model not found, waiting or validation failed:", e)
    # create a dummy prediction logic if needed, but for now let's fail or wait
    # In real world, we might wait. Here let's proceed to fail if no model.
    pass

# SCHEMA
schema = StructType() \
    .add("id", StringType()) \
    .add("ingestion_timestamp", LongType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("time", StringType()) \
    .add("temperature", DoubleType()) \
    .add("windspeed", DoubleType()) \
    .add("winddirection", DoubleType()) \
    .add("weathercode", IntegerType()) \
    .add("is_day", IntegerType())

# READ KAFKA
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-bd:9092") \
    .option("subscribe", "weather_raw") \
    .load()

parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# INFERENCE STREAM
# Spark ML streaming requires transformation on DataFrame
# Transform applies the vector assembler and model
predictions = model.transform(parsed_df)

# SELECT OUTPUT
output_df = predictions.select(
    col("id"),
    col("time"),
    col("temperature").alias("actual_temp"),
    col("prediction").alias("predicted_temp"),
    col("ingestion_timestamp")
)

# WRITE TO KAFKA topic 'weather_predictions'
query = output_df.select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-bd:9092") \
    .option("topic", "weather_predictions") \
    .option("checkpointLocation", "/tmp/checkpoints/predictions") \
    .outputMode("append") \
    .start()

query.awaitTermination()
