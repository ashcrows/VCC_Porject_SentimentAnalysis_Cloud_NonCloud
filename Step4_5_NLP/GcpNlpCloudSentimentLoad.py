import os
from dotenv import load_dotenv
import mysql.connector
from google.cloud import language_v1
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from datetime import datetime

# === Load environment ===
load_dotenv()
print("🔐 Using credentials from:", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

# === Configuration ===
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
INPUT_TABLE = "gcp_hospital_sentiment"
OUTPUT_TABLE = "nonCloudSentiments"
JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"

# === Initialize Spark ===
spark = SparkSession.builder.appName("NonCloudSentimentAnalysis").config(
    "spark.jars.packages", "mysql:mysql-connector-java:8.0.33"  # Or your MySQL connector version
).getOrCreate()
print("✅ Spark session started")

# === Read from MySQL using JDBC ===
try:
    df_reviews = spark.read.format("jdbc").options(
        url=JDBC_URL,
        dbtable=INPUT_TABLE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        driver=JDBC_DRIVER,
    ).load().select("hospital_name", "address", "place_id", "hospital_rating", "author_name", "author_id", "review", "sentiment")
    print(f"✅ Read {df_reviews.count()} rows from {INPUT_TABLE}")
except Exception as e:
    print(f"❌ Error reading from MySQL: {e}")
    spark.stop()
    exit(1)

# === GCP Sentiment function ===
client = language_v1.LanguageServiceClient()

def analyze_sentiment_gcp(text):
    document = language_v1.Document(
        content=text,
        type_=language_v1.Document.Type.PLAIN_TEXT
    )
    response = client.analyze_sentiment(request={"document": document})
    score = response.document_sentiment.score

    if score > 0.25:
        label = "Positive"
    elif score < -0.25:
        label = "Negative"
    else:
        label = "Mixed"

    return label, score

# Define the UDF output schema
sentiment_schema = StructType([
    StructField("non_cloud_sentiment", StringType()),
    StructField("non_cloud_sentiment_score", FloatType())
])

# === Analyze sentiment using UDF ===
analyze_sentiment_udf = udf(analyze_sentiment_gcp, sentiment_schema)

df_with_sentiment = df_reviews.filter((col("sentiment").isNull()) | (col("sentiment") == "")) \
    .withColumn("sentiment_results", analyze_sentiment_udf(col("review"))) \
    .withColumn("non_cloud_sentiment", col("sentiment_results.non_cloud_sentiment")) \
    .withColumn("non_cloud_sentiment_score", col("sentiment_results.non_cloud_sentiment_score")) \
    .drop("sentiment_results") \
    .withColumn("LoadDateTime", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

output_df = df_with_sentiment.select("hospital_name", "address", "place_id", "hospital_rating", "author_name", "author_id", "review", "sentiment", "non_cloud_sentiment", "non_cloud_sentiment_score", "LoadDateTime")

print(f"✅ Analyzed sentiment for {output_df.count()} reviews.")

# === Write to MySQL using JDBC ===
try:
    output_df.write.format("jdbc").options(
        url=JDBC_URL,
        dbtable=OUTPUT_TABLE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        driver=JDBC_DRIVER,
        mode="append"  # Or "overwrite" depending on your needs
    ).save()
    print(f"✅ Wrote sentiment data to {OUTPUT_TABLE}")
except Exception as e:
    print(f"❌ Error writing to MySQL: {e}")

# === Stop Spark ===
spark.stop()
print("✅ Spark session stopped.")