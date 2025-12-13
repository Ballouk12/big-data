from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline


# HBASE CONFIG
HBASE_HOST = 'hbase-standalone-bd'
HBASE_PORT = 9090
TABLE_NAME = 'weather_history'
MODEL_PATH = "hdfs://namenode-bd:9000/models/weather_v1"

spark = SparkSession.builder \
    .appName("BatchMLTrain") \
    .getOrCreate()

# LOAD DATA FROM HBASE REST
HBASE_REST_URL = "http://hbase-standalone-bd:8000"

def get_data_from_hbase():
    import requests
    import json
    import base64
    
    # Scan via REST: GET /<table>/*
    # This might return XML/JSON. We ask for JSON.
    headers = {"Accept": "application/json"}
    
    rows = []
    try:
        # Limit scan for demo performance
        # /table/scanner endpoint is better for large data but simple GET /* works for small tables
        r = requests.get(f"{HBASE_REST_URL}/{TABLE_NAME}/*", headers=headers)
        if r.status_code == 200:
            data = r.json()
            # Parse rows
            # Structure: {"Row": [{"key": "...", "Cell": [{"column": "...", "$": "..."}]}]}
            for row_item in data.get("Row", []):
                vals = {}
                for cell in row_item.get("Cell", []):
                    col = base64.b64decode(cell['column']).decode('utf-8')
                    val = base64.b64decode(cell['$']).decode('utf-8')
                    vals[col] = val
                
                try:
                    t = float(vals.get('data:temperature', 0))
                    w = float(vals.get('data:windspeed', 0))
                    d = float(vals.get('data:winddirection', 0))
                    i = int(vals.get('data:is_day', 0))
                    rows.append((t, w, d, i))
                except:
                    continue
    except Exception as e:
        print(f"Error scanning HBase REST: {e}")
            
    return rows

data = get_data_from_hbase()
if not data:
    print("No data in HBase to train.")
    exit(0)

df = spark.createDataFrame(data, ["temperature", "windspeed", "winddirection", "is_day"])

# FEATURE ENGINEERING
assembler = VectorAssembler(
    inputCols=["windspeed", "winddirection", "is_day"],
    outputCol="features"
)

# MODEL (Predict Temperature based on others)
lr = LinearRegression(featuresCol="features", labelCol="temperature")

pipeline = Pipeline(stages=[assembler, lr])

# TRAIN
model = pipeline.fit(df)

# SAVE MODEL TO HDFS
model.write().overwrite().save(MODEL_PATH)
print("Model saved to", MODEL_PATH)

# SHOW METRICS
predictions = model.transform(df)
predictions.select("features", "temperature", "prediction").show(5)

spark.stop()
