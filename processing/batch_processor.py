import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, FloatType
from pyspark.sql import Row
from pyspark.sql.functions import col, current_timestamp, from_json, coalesce, lit, when, to_date
from pyspark.sql.functions import max as _max
    
MONGO_SERVER = os.getenv("MONGO_SERVER", "mongodb://mongodb:27017/uma_db.uma_historical_club_stats")

DATABASE = "uma_db"
DB_COLLECTION = "historical_club_stats"

# Specifies the MongoDB Spark connector version below.
spark = (
    SparkSession.builder # no .master, it is specified when running the program with the --master flag.
    .appName("UmaMoeBatchProcessor")
    .config(
        "spark.jars.packages", 
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0," # Running spark 4.2.0. ',' is inside as they both are being sent together (not as 3 arguments! only 2)
        "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0"
    )
    .config("spark.mongodb.read.connection.uri", MONGO_SERVER)
    .config("spark.mongodb.write.connection.uri", MONGO_SERVER)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN") # Less information than default to logs, makes it more readable.

# fetches the latest kafka_published_at timestamp in MongoDB, to avoid appending duplicates (assuming same timestamp. Otherwise yes to 'duplicate', as historical data.)
try:
    existing_mongo_df = (
        spark.read
        .format("mongodb")
        .option("database", DATABASE)
        .option("collection", DB_COLLECTION)
        .load()
        .select("circle_id", "date_updated")
    )
    #latest_ts = existing_mongo_df.select(_max("kafka_published_at")).collect()[0][0] # collect returns a list, we enter that list to get the row, then we enter the row to get the datetime (second [0])
except Exception:
    existing_mongo_df = None # Empty collection
    #latest_ts = None

club_schema = StructType([
    StructField("circle_id", IntegerType(), False), # Not nullable (therefore False), primary key (along with timestamp) for the club data.
    StructField("name", StringType(), True),
    StructField("member_count", IntegerType(), True),
    StructField("join_style", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("last_updated", StringType(), True),
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

# Outdated method, checks only latest timestamp and not club_id. if we were to add new clubs, it would not send anything to mongodb. latest_ts is commented out. Not best practice (could just look back in git), but whatever, just a personal note for later when I forget.
#if latest_ts is not None:
#    kafka_df = kafka_df.filter(col("timestamp") > latest_ts) # if timestamp is greater than latest timestamp, then we are good to go! (no duplicate of same time)

processed_df = (
    kafka_df

    # Cast the binary payload to string, and parse with club_schema. then add a timestamp for when the data was processed. This will help with tracking and debugging.
    .select(
        from_json(col("value").cast("string"), club_schema).alias("data"), # value is the actual json body. cast it into string, then parse it with club_schema. data is the parsed json object.
        col("timestamp").alias("kafka_published_at"),
        current_timestamp().alias("spark_processed_at")
    )
    .select("data.*", "kafka_published_at", "spark_processed_at") # data.* to expand the json body into individual dataframe columns, as well as add columns for kafka_published_at and spark_processed_at.

    # Get the date the API updated the club data. That way, can avoid (daily) duplicates. Considered using kafka_published_at, but if there's a delay then it won't be the correct date!
    .withColumn("date_updated", to_date(col("last_updated")))

    # Handling null values and removing rows we don't need. Need to handle potential null values so they don't mess up potential calcs in PowerBI.
    .drop("live_points", "live_rank", "last_live_update") # Drop the live points and rank columns, not for historical data or batch processing. using later for real-time streaming
    .filter(col("circle_id").isNotNull()) # Handling missing data and NULL values. Fills with default values
    .withColumn("member_count", coalesce(col("member_count"), lit(0)))  # if member_count is null, fill with 0
    .withColumn("monthly_point", coalesce(col("monthly_point"), lit(0)))
    .withColumn("last_month_point", coalesce(col("last_month_point"), lit(0)))

    # Transforming some data within the batch (Mostly ones involving static values. Calculations involving dynamic values (e.g. avg_fans_per_member) is done through PowerBI and DAX.)
    # .withColumn("avg_fans_per_member", when(col("member_count") > 0, col("monthly_point") / col("member_count")).otherwise(0.0))
    # .withColumn("avg_fans_per_member_last_month", when(col("member_count") > 0, col("last_month_point") / col("member_count")).otherwise(0.0)) requires me to fetch member_count for last month too from another GET call. Better to do in DAX in PowerBI (better for multi-source data, and historical data. using pyspark for single-batch processing.)
    .withColumn("is_full", col("member_count") >= 30) # Sometimes member_count is above 30, due to counting members who have left the same month.
    .withColumn("point_difference_last_month", when((col("monthly_point") > 0) & (col("last_month_point") > 0), col("monthly_point") - col("last_month_point")).otherwise(None)) # Using when to handle null values
    .withColumn("rank_difference_last_month", when((col("monthly_rank").isNotNull()) & (col("last_month_rank").isNotNull()), col("monthly_rank") - col("last_month_rank")).otherwise(None))
    
    # should help with idempotency and avoiding duplicate data (within the same batch)
    .dropDuplicates(["circle_id", "kafka_published_at"])
)

# Performing left anti-join against MongoDB records
if existing_mongo_df is not None:
    unwritten_clubs_df = processed_df.join(
        existing_mongo_df, # right side of join
        on=["circle_id", "date_updated"], # columns its using to join
        how="left_anti" # any columns that didnt join (arent appended today in the database)
    )
else:
    unwritten_clubs_df = processed_df

# Not using upsert to keep historical data.
unwritten_clubs_df.write \
    .format("mongodb") \
    .mode("append") \
    .option("database", DATABASE) \
    .option("collection", DB_COLLECTION) \
    .save()

# Need to automize this somehow to launch daily eventually.