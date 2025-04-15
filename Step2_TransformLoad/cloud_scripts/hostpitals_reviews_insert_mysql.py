import json
import glob
from datetime import datetime
import mysql.connector
import hashlib
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, hash as spark_hash

# --- Configuration ---
PROJECT_ID = "g24ai1067-vcc-assignment"
BUCKET_NAME = "hospital_files_project"
JSON_FOLDER_PATH = "hospitals_review/"
MYSQL_HOST = "10.76.144.3"
MYSQL_USER = "selfroot"
MYSQL_PASSWORD = "self@Root1"
MYSQL_DATABASE = "hospital"
MYSQL_PORT = 3306
TABLE_NAME = "hospitals_reviews_ingestion"
JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"

# --- Initialize Spark ---
spark = SparkSession.builder.appName("HospitalReviewIngestion").config(
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
df_exploded = df_raw.select(explode(col("data")).alias("hospital_data"))

df_reviews = df_exploded.select(
    col("hospital_data.name").alias("hospital_name"),
    col("hospital_data.formatted_address").alias("address"),
    col("hospital_data.place_id").alias("place_id"),
    col("hospital_data.rating").alias("hospital_rating"),
    explode(col("hospital_data.reviews")).alias("review")
).select(
    "hospital_name",
    "address",
    "place_id",
    "hospital_rating",
    col("review.author_name").alias("author_name"),
    spark_hash(col("review.author_name")).cast("string").alias("author_id"),  # Use Spark's hash function
    col("review.author_url").alias("author_url"),
    col("review.language").alias("language"),
    col("review.original_language").alias("original_language"),
    col("review.profile_photo_url").alias("profile_photo_url"),
    col("review.rating").alias("review_rating"),
    col("review.relative_time_description").alias("relative_time_description"),
    col("review.text").alias("text"),
    (col("review.time").cast("timestamp")).alias("time"),
    (col("review.translated").cast("int")).alias("translated"),
    lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("LoadDateTime"),
    lit("").alias("sentiments")  # Placeholder for sentiments
)

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
        df_reviews.write.format("jdbc").options(
            url=JDBC_URL,
            dbtable=TABLE_NAME,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            driver=JDBC_DRIVER,
            batchsize=1000,
        ).mode("append").save()
        print("✅ Reviews data loaded into MySQL using JDBC")
    except Exception as e:
        print(f"❌ Error loading data into MySQL using JDBC: {e}")
else:
    print("Skipping data loading as the table does not exist.")

# --- Stop Spark ---
spark.stop()
print("✅ Spark session stopped.")