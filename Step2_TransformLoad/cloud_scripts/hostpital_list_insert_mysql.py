import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, concat_ws, lit
from datetime import datetime
from google.cloud import storage

# Replace with your GCP project ID, bucket name, and MySQL connection details
PROJECT_ID = "g24ai1067-vcc-assignment"
BUCKET_NAME = "hospital_files_project"
JSON_FOLDER_PATH = "hospital_lists/"  # Path within the bucket where JSON files are located
MYSQL_HOST = "10.76.144.3"  # Or hostname if configured
MYSQL_USER = "selfroot"
MYSQL_PASSWORD = "self@Root1"
MYSQL_DATABASE = "hospital"
MYSQL_PORT = 3306
TABLE_NAME = "hospital_lists_ingestion"
JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"  # Or the appropriate driver for your MySQL version


# --- Initialize Spark ---
spark = SparkSession.builder.appName("HospitalDataLoad").config(
    "spark.jars.packages", "mysql:mysql-connector-java:8.0.33"  # Or your MySQL connector version
).getOrCreate()
print("✅ Spark session started")

# --- Load JSON Data from GCS ---
try:
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    json_files = [
        f"gs://{BUCKET_NAME}/{blob.name}"
        for blob in bucket.list_blobs(prefix=JSON_FOLDER_PATH)
        if blob.name.endswith(".json")
    ]
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in gs://{BUCKET_NAME}/{JSON_FOLDER_PATH}")
    df_raw = spark.read.option("multiline", "true").json(json_files)
    print(f"✅ Loaded JSON files from GCS: {len(json_files)}")
except Exception as e:
    print(f"❌ Error loading JSON from GCS: {e}")
    spark.stop()
    exit(1)

# --- Transform Data ---
df = df_raw.select(explode(col("results")).alias("hospital")).select("hospital.*")
df_final = df.select(
    "name",
    "business_status",
    col("formatted_address").alias("address"),
    col("geometry.location.lat").alias("latitude"),
    col("geometry.location.lng").alias("longitude"),
    col("opening_hours.open_now").alias("open_now"),
    "rating",
    "user_ratings_total",
    concat_ws(",", col("types")).alias("types"),
    "place_id",
    lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("LoadDateTime"),
).dropna(subset=["name", "rating", "address", "latitude", "longitude"])

# --- Check if Table Exists (using MySQL Connector) ---
table_exists = False
try:
    conn = mysql.connector.connect(
        host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DATABASE, port=MYSQL_PORT
    )
    cursor = conn.cursor()
    cursor.execute(f"SHOW TABLES LIKE '{TABLE_NAME}'")
    if cursor.fetchone():
        table_exists = True
        print(f"✅ Table '{TABLE_NAME}' exists.")
    else:
        print(f"⚠️ Table '{TABLE_NAME}' does not exist. Ensure it is created before running.")
    cursor.close()
    conn.close()
except mysql.connector.Error as err:
    print(f"❌ MySQL error checking table: {err}")
    spark.stop()
    exit(1)

# --- Load Data into MySQL using JDBC ---
if table_exists:
    try:
        df_final.write.format("jdbc").options(
            url=JDBC_URL,
            dbtable=TABLE_NAME,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            driver=JDBC_DRIVER,
            batchsize=1000,  # Adjust batch size as needed
        ).mode("append").save()  # Use "append" to add data, "overwrite" to replace
        print("✅ Data loaded into MySQL using JDBC")
    except Exception as e:
        print(f"❌ Error loading data into MySQL using JDBC: {e}")
else:
    print("Skipping data loading as the table does not exist.")

# --- Stop Spark ---
spark.stop()
print("✅ Spark session stopped.")