from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, desc
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("TgCount") \
    .master("local[*]") \
    .getOrCreate()

schema = StructType() \
    .add("username", StringType()) \
    .add("time", DoubleType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "telegram_data") \
    .load()

data = df.select(from_json(col("value").cast("string"), schema).alias("json")) \
    .select("json.*") \
    .withColumn("timestamp", col("time").cast("timestamp"))

q1 = data.groupBy(window("timestamp", "1 minute", "30 seconds"), "username") \
    .count().orderBy(desc("count")) \
    .writeStream.outputMode("complete") \
    .format("console").start()

q2 = data.groupBy(window("timestamp", "10 minutes", "30 seconds"), "username") \
    .count().orderBy(desc("count")) \
    .writeStream.outputMode("complete") \
    .format("console").start()

spark.streams.awaitAnyTermination()
