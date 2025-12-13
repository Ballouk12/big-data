from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, IntegerType


# HBASE CONFIG
HBASE_HOST = 'hbase-standalone-bd'
HBASE_PORT = 9090
TABLE_NAME = 'weather_history'

def write_to_hbase(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    
    
    def process_partition(iterator):
        try:
            import happybase
            connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=60000)
            table = connection.table(TABLE_NAME)
            batch = table.batch()
            
            count = 0
            for row in iterator:
                try:
                    # RowKey: lat#lon#time
                    lat = str(row.latitude)
                    lon = str(row.longitude)
                    ts = str(row.time)
                    row_key = f"{lat}#{lon}#{ts}".encode()
                    
                    data = {
                        b'data:temperature': str(row.temperature).encode(),
                        b'data:windspeed': str(row.windspeed).encode(),
                        b'data:winddirection': str(row.winddirection).encode(),
                        b'data:weathercode': str(row.weathercode).encode(),
                        b'data:is_day': str(row.is_day).encode(),
                        b'data:time': str(row.time).encode()
                    }
                    batch.put(row_key, data)
                    count += 1
                except Exception as row_e:
                    print(f"Error processing row: {row_e}")
            
            batch.send()
            connection.close()
            print(f"Batch processed {count} rows on worker.")
            
        except Exception as e:
            print(f"Worker Hbase Connection Error: {e}")

    batch_df.foreachPartition(process_partition)

# SPARK SESSION
spark = SparkSession.builder \
    .appName("KafkaToHBase") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()

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
    .option("startingOffsets", "earliest") \
    .load()

# PARSE JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# WRITE STREAM
query = parsed_df.writeStream \
    .foreachBatch(write_to_hbase) \
    .outputMode("append") \
    .start()

query.awaitTermination()
