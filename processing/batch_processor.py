import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, FloatType
from pyspark.sql import Row
from pyspark.sql.functions import col, current_timestamp
    
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
MONGO_SERVER = os.getenv("MONGO_SERVER", "mongodb://mongodb:27017/uma_db.uma_top_clubs")

DATABASE = "uma_db"
DB_COLLECTION = "top_clubs"

spark = SparkSession.builder \
    .master(SPARK_MASTER) \
    .appName("UmaMoeBatchProcessor") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0") \
    .config("spark.mongodb.write.connection.uri", MONGO_SERVER) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") # Less information than default to logs, makes it more readable.
