from pyspark.sql import SparkSession
import happybase

# HBASE CONFIG
HBASE_HOST = 'hbase-standalone-bd'
HBASE_PORT = 9090
TABLE_NAME = 'connectivity_test'

def verify_hbase():
    print(f"Connecting to HBase at {HBASE_HOST}:{HBASE_PORT}...")
    try:
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
        print("Connection successful.")
        
        # Create table
        if TABLE_NAME.encode() not in connection.tables():
            print(f"Creating table {TABLE_NAME}...")
            connection.create_table(TABLE_NAME, {'cf': dict()})
        else:
            print(f"Table {TABLE_NAME} exists.")
            
        # Put Data
        table = connection.table(TABLE_NAME)
        print("Writing test row 'row1'...")
        table.put(b'row1', {b'cf:col1': b'value1'})
        
        # Get Data
        print("Reading test row 'row1'...")
        row = table.row(b'row1')
        print(f"Read row: {row}")
        
        if row[b'cf:col1'] == b'value1':
            print("VERIFICATION SUCCESS: Data written and read correctly.")
        else:
            print("VERIFICATION FAILED: Data mismatch.")
            
        connection.close()
    except Exception as e:
        print(f"VERIFICATION FAILED: {e}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("HBaseVerification").getOrCreate()
    # We run the verification in the driver for simplicity, confirming network/libs
    verify_hbase()
    spark.stop()
