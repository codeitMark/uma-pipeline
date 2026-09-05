import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, FloatType
from pyspark.sql import Row
from pyspark.sql.functions import col, current_timestamp
    
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
MONGO_SERVER = os.getenv("MONGO_SERVER", "mongodb://mongodb:27017/uma_db.uma_historical_club_stats")

DATABASE = "uma_db"
DB_COLLECTION = "historical_club_stats"

spark = SparkSession.builder \
    .master(SPARK_MASTER) \
    .appName("UmaMoeBatchProcessor") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0") #Specifies the MongoDB Spark connector version \
    .config("spark.mongodb.write.connection.uri", MONGO_SERVER) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") # Less information than default to logs, makes it more readable.

club_schema = StructType([
    StructField("circle_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("member_count", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("monthly_rank", IntegerType(), True),
    StructField("monthly_point", IntegerType(), True),
    StructField("last_month_rank", IntegerType(), True),
    StructField("last_month_point", IntegerType(), True),
    StructField("live_points", IntegerType(), True),
    StructField("live_rank", IntegerType(), True),
    StructField("last_live_update", StringType(), True),
    StructField("club_rank", IntegerType(), True)
])

kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "uma_top_clubs") \
    .option("startingOffsets", "earliest") \
    .load()

# Cast the binary payload to string, and parse with club_schema. then add a timestamp for when the data was processed. This will help with tracking and debugging.
parsed_df = kafka_df.select \
    .select(
        from_json(col("value").cast("string"), club_schema).alias("data"), # value is the actual json body. cast it into string, then parse it with club_schema. data is the parsed json object.
        col("timestamp").alias("kafka_published_at"),
        current_timestamp().alias("spark_processed_at")
    ) \
    .select("data.*", "kafka_published_at", "spark_processed_at") # data.* to expand the json body into individual dataframe columns.
    
batch_df = parsed_df.drop("live_points", "live_rank", "last_live_update") # drop the live points and rank columns, not for historical data or batch processing. using later for real-time streaming

# For if producer pushed duplicate data
final_batch_df = batch_df.dropDuplicates(["circle_id", "kafka_published_at"]) # should help with idempotency and avoiding duplicate data.

final_batch_df.write \
    .format("mongodb") \
    .mode("append") # Not using upsert to keep historical data. \
    .option("database", DATABASE) \
    .option("collection", DB_COLLECTION) \
    .save()