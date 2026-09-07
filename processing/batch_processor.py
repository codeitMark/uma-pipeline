import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, current_timestamp, from_json, coalesce, lit, when, to_date, date_format, explode, size, aggregate
from pyspark.sql.functions import max as _max, round as spark_round, filter as _filter
from schemas import club_schema, threshold_schema, member_schema
    
MONGO_SERVER = os.getenv("MONGO_URI", "mongodb://mongodb:27017/uma_db.uma_historical_club_stats")
KAFKA_BOOTSTRAP = "kafka:9092"
DATABASE = "uma_db"

CLUBS_TOPIC = "uma_top_clubs"
THRESHOLDS_TOPIC = "rank_thresholds"
MEMBERS_TOPIC = "club_members"

# fetches the latest kafka_published_at timestamp in MongoDB, to avoid appending duplicates (assuming same timestamp. Otherwise yes to 'duplicate', as historical data.)
def read_existing_db(spark, db_collection):
    try:
        existing_mongo_df = (
            spark.read
            .format("mongodb")
            .option("database", DATABASE)
            .option("collection", db_collection)
            .load()
            #.select("circle_id", "date_updated") # causes UNRESOVED_COLUMN if empty
        )
        #latest_ts = existing_mongo_df.select(_max("kafka_published_at")).collect()[0][0] # collect returns a list, we enter that list to get the row, then we enter the row to get the datetime (second [0])
        return existing_mongo_df
    except Exception as e:
        print(f"MongoDB read failed: {e}")
        return None

# latest startingoffset or earliest
def read_kafka_topic(spark, topic):
    kafka_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()
    return kafka_df

# Outdated method, checks only latest timestamp and not club_id. if we were to add new clubs, it would not send anything to mongodb. latest_ts is commented out. Not best practice (could just look back in git), but whatever, just a personal note for later when I forget.
#       if latest_ts is not None:
#           kafka_df = kafka_df.filter(col("timestamp") > latest_ts) # if timestamp is greater than latest timestamp, then we are good to go! (no duplicate of same time)

def parse_df(kafka_df, schema):
    parsed_df = (kafka_df
            # Cast the binary payload to string, and parse with club_schema. then add a timestamp for when the data was processed. This will help with tracking and debugging.
        .select(
            from_json(col("value").cast("string"), schema).alias("data"), # value is the actual json body. cast it into string, then parse it with club_schema. data is the parsed json object.
            col("timestamp").alias("kafka_published_at"),
            current_timestamp().alias("spark_processed_at")
        )
        .select("data.*", "kafka_published_at", "spark_processed_at") # data.* to expand the json body into individual dataframe columns, as well as add columns for kafka_published_at and spark_processed_at.
    )

    return parsed_df

def unwritten_club_records(existing_mongo_df, processed_df):
    # Performing left anti-join against MongoDB records
    if (existing_mongo_df is not None and "circle_id" in existing_mongo_df.columns and "date_updated" in existing_mongo_df.columns):
        existing_keys = existing_mongo_df.select("circle_id", "date_updated").distinct() # distinct to limit the comparisons the database does, as circle_id and date_updated are meant to be the primary key together.

        unwritten_clubs_df = processed_df.join(
            existing_mongo_df, # right side of join
            on=["circle_id", "date_updated"], # columns its using to join
            how="left_anti" # any columns that didnt join (arent appended today in the database)
        )
    else:
        # Empty collection
        unwritten_clubs_df = processed_df
    
    return unwritten_clubs_df

def unwritten_members_records(existing_mongo_df, processed_df):
    if (existing_mongo_df is not None and "id" in existing_mongo_df.columns and "date_updated" in existing_mongo_df.columns):
        existing_keys = existing_mongo_df.select("id", "date_updated").distinct()

        unwritten_members_df = processed_df.join(
            existing_mongo_df,
            on=["id", "date_updated"],
            how="left_anti"
        )
    else:
        unwritten_members_df = processed_df
    
    return unwritten_members_df

def unwritten_thresholds_records(existing_mongo_df, processed_df):
    # Problem: no date_updated from the API. I will have to use records I expect to change. There is an edge case where yesterday_min_fans and current_min_fans are the same as yesterdays (and therefore the same, crazy consistency) while it's another day (different date_streamed). date_streamed because of potential delays, as updated (according to API) isnt really accurate.
    if (existing_mongo_df is not None and "yesterday_min_fans" in existing_mongo_df.columns and "current_min_fans" in existing_mongo_df.columns and "date_streamed" in existing_mongo_df.columns):
        existing_keys = existing_mongo_df.select("yesterday_min_fans", "current_min_fans", "date_streamed").distinct()

        unwritten_thresholds_df = processed_df.join(
            existing_mongo_df,
            on=["yesterday_min_fans", "current_min_fans", "date_streamed"],
            how="left_anti"
        )
    else:
        unwritten_thresholds_df = processed_df
    
    return unwritten_thresholds_df

def write_to_mongodb(unwritten_df, db_collection):
    # Not using upsert to keep historical data.
    if unwritten_df.rdd.isEmpty():
        print("Dataframes is empty, records are likely already in MongoDB. If so, then daily snapshot has already been performed.")
    else:
        unwritten_df.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", DATABASE) \
            .option("collection", db_collection) \
            .save()
        print("Written collection/records into mongodb.")

def process_club_data(spark):
    db_collection = "historical_club_stats"
    existing_mongo_df = read_existing_db(spark, db_collection)

    kafka_df = read_kafka_topic(spark, CLUBS_TOPIC)

    parsed_df = parse_df(kafka_df, club_schema)

    processed_df = (
        parsed_df

        # Get the date the API updated the club data. That way, can avoid (daily) duplicates. Considered using kafka_published_at, but if there's a delay then it won't be the correct date!
        .withColumn("date_updated", date_format(col("last_updated"), "yyyy-MM-dd")) # ISO-standard formatting. Saved as a string, to avoid time (since it is actually saving time (clock) as well)

        # Handling null values and removing rows we don't need. Need to handle potential null values so they don't mess up potential calcs in PowerBI.
        .drop("live_points", "live_rank", "last_live_update") # Drop the live points and rank columns, not for historical data or batch processing. using later for real-time streaming
        .filter(col("circle_id").isNotNull()) # Handling missing data and NULL values. Fills with default values
        .withColumn("member_count", coalesce(col("member_count"), lit(0)))  # if member_count is null, fill with 0
        .withColumn("monthly_point", coalesce(col("monthly_point"), lit(0)))
        .withColumn("last_month_point", coalesce(col("last_month_point"), lit(0)))

        # Transforming some data within the batch (Mostly ones involving static values. Calculations involving dynamic values (e.g. avg_fans_per_member across multiple clubs) is done through PowerBI and DAX.)
        .withColumn("monthly_fans_per_member", when(col("member_count") > 0, spark_round(col("monthly_point") / col("member_count"), 2)).otherwise(0.0))
        .withColumn("is_full", col("member_count") >= 30) # Sometimes member_count is above 30, due to counting members who have left the same month.
        # .withColumn("monthly_fans_per_member_last_month", when(col("member_count") > 0, col("last_month_point") / col("member_count")).otherwise(0.0)) requires me to fetch member_count for last month too from another GET call. Better to do in DAX in PowerBI (better for multi-source data, and historical data. using pyspark for single-batch processing.)

        .withColumn("point_difference_last_month", when((col("monthly_point") > 0) & (col("last_month_point") > 0), col("monthly_point") - col("last_month_point")).otherwise(None)) # Using when to handle null values
        .withColumn("rank_difference_last_month", when((col("monthly_rank").isNotNull()) & (col("last_month_rank").isNotNull()), col("monthly_rank") - col("last_month_rank")).otherwise(None))

        # Counting 30+ as 30 members, as max is 30. Transformation for visualization, sort for sortability in PowerBI.
        .withColumn("member_count_range", when(col("member_count") <= 10, "0-10").when((col("member_count") > 10) & (col("member_count") <= 20), "11-20").when((col("member_count") > 20), "21-30"))
        .withColumn("member_count_range_sort", when(col("member_count_range") == "0-10", 1).when(col("member_count_range") == "11-20", 2).when(col("member_count_range") == "21-30", 3))

        # should help with idempotency and avoiding duplicate data (within the same batch)
        .dropDuplicates(["circle_id", "date_updated"])
    )

    unwritten_clubs_df = unwritten_club_records(existing_mongo_df, processed_df)

    write_to_mongodb(unwritten_clubs_df, db_collection)

def process_member_data(spark):
    db_collection = "member_data"
    existing_mongo_df = read_existing_db(spark, db_collection)

    kafka_df = read_kafka_topic(spark, MEMBERS_TOPIC)

    parsed_df = parse_df(kafka_df, member_schema)

    processed_df = (
        parsed_df
        .withColumn("date_updated", date_format(col("last_updated"), "yyyy-MM-dd"))

        .withColumn("total_current_month_fans", aggregate(col("daily_fans"), lit(0).cast("long"), lambda total, daily: total + daily))
        
        .withColumn("total_active_days", size(_filter(col("daily_fans"), lambda fans: fans > 0)))

        .withColumn("changed_club", col("previous_circle_id").isNotNull() & (col("previous_circle_id") != col("circle_id")))
        .dropDuplicates(["id", "date_updated"])
    )

    unwritten_members_df = unwritten_members_records(existing_mongo_df, processed_df)

    write_to_mongodb(unwritten_members_df, db_collection)

def process_rank_thresholds(spark):
    db_collection = "rank_thresholds_stats"
    existing_mongo_df = read_existing_db(spark, db_collection)

    kafka_df = read_kafka_topic(spark, THRESHOLDS_TOPIC)

    parsed_df = parse_df(kafka_df, threshold_schema)

    # Shouldn't require any null handling tbh
    processed_df = (
        parsed_df
        .withColumn("date_streamed", to_date(col("kafka_published_at")))
        .withColumn("rank_range", col("ranking_to") - col("ranking_from"))

        .dropDuplicates(["yesterday_min_fans", "current_min_fans", "date_streamed"])
    )

    unwritten_thresholds_df = unwritten_thresholds_records(existing_mongo_df, processed_df)
    
    write_to_mongodb(unwritten_thresholds_df, db_collection)


def main():
    parser = argparse.ArgumentParser(description="PySpark batch processor, from Kafka to MongoDB ETL")
    parser.add_argument(
        "--target", 
        choices=["clubs", "thresholds", "members", "all"],
        required=True, 
        help="Use flag --target <data>. data being which pipeline producer. Options are clubs, thresholds, members, and all."
    )
    
    args = parser.parse_args()

    # Specifies the MongoDB Spark connector version below.
    spark = (
        SparkSession.builder # no .master, it is specified when running the program with the --master flag.
        .appName("UmaMoeBatchProcessor")
        .config(
            "spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0," # Running spark 4.2.0. ',' is inside as they both are being sent together (not as 3 arguments! only 2)
            "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0" # Not necessary as they are specified in flags during launch, because this gets ignored for some reason.
        )
        .config("spark.mongodb.read.connection.uri", MONGO_SERVER)
        .config("spark.mongodb.write.connection.uri", MONGO_SERVER)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN") # Less information than default to logs, makes it more readable. Seems to be getting ignored though.

    if args.target == "clubs":
        process_club_data(spark)
    elif args.target == "thresholds":
        process_rank_thresholds(spark)
    elif args.target == "members":
        process_member_data(spark)
    elif args.target == "all":
        process_club_data(spark)
        process_member_data(spark)
        process_rank_thresholds(spark)

    spark.stop()

if __name__ == "__main__":
    main()

# Need to automize this somehow to launch daily eventually.
# Considering fetching data on every member for every club, but that will make api calls take crazy long. According to Gemini it will also introduce slower producer runtimes, as it introduces the "N+1 query problem" Common performance bottleneck. Seems more complicated, so I will do it after I get to visualize data on PowerBI and handle real-time streaming.