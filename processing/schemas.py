from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, StringType, LongType

club_schema = StructType([
    StructField("circle_id", IntegerType(), False), # Not nullable (therefore False), primary key (along with timestamp) for the club data.
    StructField("name", StringType(), True),
    StructField("member_count", IntegerType(), True),
    StructField("join_style", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("last_updated", StringType(), True),
    StructField("monthly_rank", IntegerType(), True), # Not sure how uma.moe handles new clubs, as they are unranked during their 1st month. They seem to still be ranking them though.
    StructField("monthly_point", LongType(), True),
    StructField("last_month_rank", IntegerType(), True),
    StructField("last_month_point", LongType(), True),
    StructField("live_points", LongType(), True),
    StructField("live_rank", IntegerType(), True),
    StructField("last_live_update", StringType(), True),
    StructField("club_rank", IntegerType(), True)
])

threshold_schema = StructType([
    StructField("rank_index", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("ranking_from", IntegerType(), False),
    StructField("ranking_to", IntegerType(), False),
    StructField("current_min_fans", LongType(), True),
    StructField("current_fans_per_day", LongType(), True),
    StructField("yesterday_min_fans", LongType(), True),
    StructField("yesterday_fans_per_day", LongType(), True),
    StructField("daily_fans_delta", LongType(), True),
    StructField("last_month_min_fans", LongType(), True),
    StructField("last_month_fans_per_day", LongType(), True),
    StructField("current_vs_last_month_delta", LongType(), True)
])

member_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("circle_id", IntegerType(), True),
    StructField("viewer_id", LongType(), True),
    StructField("trainer_name", StringType(), True),
    StructField("shame_score", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("daily_fans", ArrayType(LongType()), True),
    StructField("last_updated", StringType(), True),
    StructField("previous_circle_id", IntegerType(), True),
    StructField("previous_circle_name", StringType(), True),
    StructField("next_month_start", LongType(), True)
])