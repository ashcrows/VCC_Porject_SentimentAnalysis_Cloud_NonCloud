import os
from dotenv import load_dotenv
import mysql.connector
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType
from datetime import datetime

# === Load environment ===
load_dotenv()
print("🔐 Using credentials from:", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

# === Configuration ===
MYSQL_HOST = "10.76.144.3"
MYSQL_USER = "selfroot"
MYSQL_PASSWORD = "self@Root1"
MYSQL_DATABASE = "hospital"
MYSQL_PORT = 3306
INPUT_TABLE = "hospitals_reviews_ingestion"  # Assuming this table exists from the previous script
OUTPUT_TABLE = "nonCloudSentiments"
JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
SENTIMENT_MODEL_NAME = "brettclaus/Hospital_Reviews"

# === Initialize Spark ===
spark = SparkSession.builder.appName("NonCloudSentimentAnalysis").config(
    "spark.jars.packages", "mysql:mysql-connector-java:8.0.33"  # Or your MySQL connector version
).getOrCreate()
print("✅ Spark session started")

# === Load Sentiment Model and Tokenizer (Run on Driver) ===
model = AutoModelForSequenceClassification.from_pretrained(SENTIMENT_MODEL_NAME)
tokenizer = AutoTokenizer.from_pretrained(SENTIMENT_MODEL_NAME)
id2label = model.config.id2label
print("✅ Sentiment model loaded on driver.")

# === Define UDF for Sentiment Analysis ===
def analyze_sentiment(review):
    if not review or not review.strip():
        return ""
    inputs = tokenizer(review, return_tensors="pt", truncation=True, padding=True)
    with torch.no_grad():
        outputs = model(**inputs)
    pred = torch.argmax(outputs.logits, dim=1).item()
    return id2label[pred]

analyze_sentiment_udf = udf(analyze_sentiment, StringType())

# === Read from MySQL using JDBC ===
try:
    df_reviews = spark.read.format("jdbc").options(
        url=JDBC_URL,
        dbtable=INPUT_TABLE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        driver=JDBC_DRIVER,
    ).load().select("hospital_name", "address", "place_id", "hospital_rating", "author_name", "author_id", "review")
    print(f"✅ Read {df_reviews.count()} rows from {INPUT_TABLE}")
except Exception as e:
    print(f"❌ Error reading from MySQL: {e}")
    spark.stop()
    exit(1)

# === Analyze Sentiment using UDF ===
df_with_sentiment = df_reviews.withColumn("non_cloud_sentiment", analyze_sentiment_udf(col("review"))) \
    .withColumn("LoadDateTime", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

output_df = df_with_sentiment.select("hospital_name", "address", "place_id", "hospital_rating", "author_name", "author_id", "review", "non_cloud_sentiment", "LoadDateTime")

print(f"✅ Analyzed sentiment for {output_df.count()} reviews.")

# === Write to MySQL using JDBC (APPEND MODE) ===
try:
    output_df.write.format("jdbc").options(
        url=JDBC_URL,
        dbtable=OUTPUT_TABLE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        driver=JDBC_DRIVER,
        mode="append"
    ).save()
    print(f"✅ Appended sentiment data to {OUTPUT_TABLE}")
except Exception as e:
    print(f"❌ Error writing to MySQL: {e}")

# === Stop Spark ===
spark.stop()
print("✅ Spark session stopped.")