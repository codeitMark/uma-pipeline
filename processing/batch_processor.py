import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, FloatType
from pyspark.sql import Row
from pyspark.sql.functions import col, current_timestamp
    
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
MONGO_SERVER = os.getenv("MONGO_SERVER", "mongodb://mongodb:27017/uma_db.uma_historical_club_stats")

DATABASE = "uma_db"
DB_COLLECTION = "historical_club_stats"

# Specifies the MongoDB Spark connector version below.
spark = SparkSession.builder \
    .master(SPARK_MASTER) \
    .appName("UmaMoeBatchProcessor") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0") \
    .config("spark.mongodb.write.connection.uri", MONGO_SERVER) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") # Less information than default to logs, makes it more readable.

club_schema = StructType([
    StructField("circle_id", IntegerType(), False), # Not nullable (therefore False), primary key for the club data.
    StructField("name", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("member_count", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("monthly_rank", IntegerType(), True), # Not sure how uma.moe handles new clubs, as they are unranked during their 1st month. They seem to still be ranking them though.
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
    .select("data.*", "kafka_published_at", "spark_processed_at") # data.* to expand the json body into individual dataframe columns, as well as add columns for kafka_published_at and spark_processed_at.
    
# Drop the live points and rank columns, not for historical data or batch processing. using later for real-time streaming
# .filter(col("circle_id").isNotNull()) is handling missing data and NULL values. Fills with default values
# coalesce and lit(0): if member_count is null, fill with 0
non-null_df = parsed_df \
        .drop("live_points", "live_rank", "last_live_update") \
        .filter(col("circle_id").isNotNull()) \
        .withColumn("member_count", coalesce(col("member_count"), lit(0))) \
        .withColumn("monthly_point", coalesce(col("monthly_rank"), lit(0))) \
        .withColumn("last_month_point", coalesce(col("last_month_point"), lit(0))) \
    
# Using when to handle null values below
# Sometimes member_count is above 30, due to counting members who have left the same month.
# .withColumn("avg_fans_per_member_last_month", when(col("member_count") > 0, col("last_month_point") / col("member_count")).otherwise(0.0)) requires me to fetch member_count for last month too from another GET call. Better to do in DAX in PowerBI (better for multi-source data, and historical data. using pyspark for single-batch processing.)

transformed_df = non-null_df \
        .withColumn("avg_fans_per_member", when(col("member_count") > 0, col("monthly_point") / col("member_count")).otherwise(0.0)) \
        .withColumn("is_full", col("member_count") >= 30) \
        .withColumn("point_difference_last_month", when(col("monthly_point") > 0 & col("last_month_point") > 0, col("monthly_point") - col("last_month_point")).otherwise(None)) \
        .withColumn("rank_difference_last_month", when(col("monthly_rank") > 0 & col("last_month_rank") > 0, col("monthly_rank") - col("last_month_rank")).otherwise(None)) \


# For if producer pushed duplicate data
final_batch_df = transformed_df.dropDuplicates(["circle_id", "kafka_published_at"]) # should help with idempotency and avoiding duplicate data.

# So many dfs. I should write it all together in one df! Refactor now, to avoid technical debt.

final_batch_df.write \
    .format("mongodb") \
    .mode("append") # Not using upsert to keep historical data. \
    .option("database", DATABASE) \
    .option("collection", DB_COLLECTION) \
    .save()

# Need to automize this somehow.