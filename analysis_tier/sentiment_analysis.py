import sys
import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
from textblob import TextBlob

# Configuration pour télécharger le connecteur Kafka-Spark automatiquement
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

# UDF d'analyse de sentiment
def analyze_sentiment(text):
    try:
        polarity = TextBlob(text).sentiment.polarity
        if polarity > 0.1: return "POSITIVE"
        elif polarity < -0.1: return "NEGATIVE"
        else: return "NEUTRAL"
    except: return "NEUTRAL"

if __name__ == "__main__":
    
    spark = SparkSession.builder\
        .appName("SocialSentimentAnalysis")\
        .master("local[*]")\
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Configuration Localhost
    bootstrapServers = "localhost:9092"
    topic = "social_data"

    # Lecture du stream
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option("subscribe", topic)\
        .option("startingOffsets", "latest")\
        .load()\
        .selectExpr("CAST(value AS STRING)")

    # Schéma des données
    schema = StructType([
        StructField("platform", StringType()),
        StructField("text", StringType()),
        StructField("creation_time", StringType())
    ])

    # Enregistrement UDF
    sentiment_udf = F.udf(analyze_sentiment, StringType())

    # Parsing et enrichissement (Event Time + Sentiment)
    # Note: On convertit creation_time en Timestamp pour le fenêtrage
    clean_df = lines.select(
        F.from_json(F.col("value"), schema).alias("data")
    ).select("data.*") \
    .withColumn("event_time", F.to_timestamp(F.col("creation_time"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("sentiment", sentiment_udf(F.col("text")))

    # Agrégation par fenêtre de temps (Windowing)
    # On groupe par tranches de 20 secondes, mises à jour toutes les 10 secondes
    sentiment_counts = clean_df \
        .withWatermark("event_time", "10 seconds") \
        .groupBy(
            F.window(F.col("event_time"), "20 seconds", "10 seconds"),
            F.col("platform"),
            F.col("sentiment")
        ).count() \
        .orderBy("window")

    # Affichage Console
    query = sentiment_counts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .option("truncate", "false")\
        .trigger(processingTime="5 seconds")\
        .start()

    query.awaitTermination()