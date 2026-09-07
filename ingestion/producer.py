import os
import json
import requests
import time
import argparse
import math
from kafka import KafkaProducer

KAFKASERVER = os.getenv("KAFKA_SERVERS", "localhost:9094")

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8') # JSON to binary data for Kafka
)

HEADERS = {
    "X-API-Key": os.getenv("UMA_API_KEY"),
    "Accept": "application/json"
}

# Defining expected Schema to address null or empty values
CLUB_SCHEMA = {
    "circle_id": 0,
    "name": "Unknown",
    "member_count": 0,
    "join_style": 0,
    "created_at": "1970-01-01T00:00:00Z",
    "last_updated": "1970-01-01T00:00:00Z",
    "monthly_rank": 0,
    "monthly_point": 0,
    "last_month_rank": 0,
    "last_month_point": 0,
    "live_points": 0,
    "live_rank": 0,
    "last_live_update": "1970-01-01T00:00:00Z",
    "club_rank": 0
} #club_rank 11 is SS, 10 is S+, 9 is S, etc.

THRESHOLD_SCHEMA = {
    "rank_index": 0, #club_rank from CLUB_SCHEMA is rank_index here.
    "name": "Unknown",
    "ranking_from": 0,
    "ranking_to": 0,
    "current_min_fans": 0,
    "current_fans_per_day": 0,
    "yesterday_min_fans": 0,
    "yesterday_fans_per_day": 0,
    "daily_fans_delta": 0,
    "last_month_min_fans": 0,
    "last_month_fans_per_day": 0,
    "current_vs_last_month_delta": 0
}

# A helper function for sanitizing club data to stream only required fields above, and fill missing fields with default values
def sanitize_json(raw_json, schema):
    # Checks if the input is a json object (dict)
    # If it is not, returns a new dict with default values from the schema
    if not isinstance(raw_json, dict):
        return {key: default for key, default in schema.items()}

    sanitized_data = {}

    for key, def_val in schema.items(): #uses only the items in schema, removes any extra fields that aren't in the schema.
        val = raw_json.get(key, def_val)

        sanitized_data[key] = def_val if val is None or val == "" else val

    return sanitized_data

#Note: Make sure to use upsert to avoid duplicates in mongodb (idempotency) (for later)
def uma_top_clubs():
    TOPIC = "uma_top_clubs"

    #Can be changed dynamically here if you wish. limit is limit per page (which is max 100.)
    sort_by = "monthly_rank"
    sort_dir = "desc"
    total_clubs = 400
    if total_clubs <= 100:
        limit = total_clubs
        club_url = f"https://uma.moe/api/v4/circles/list?page=0&limit={limit}&sort_by={sort_by}&sort_dir={sort_dir}"

        response = requests.get(club_url, headers=HEADERS)
        response_check(response, "circles", CLUB_SCHEMA, TOPIC)
    else:
        limit = 100
        rest_clubs = total_clubs % 100
        max_pages = math.floor(total_clubs / 100)
        for page in range(0, max_pages):
            club_url = f"https://uma.moe/api/v4/circles/list?page={page}&limit={limit}&sort_by={sort_by}&sort_dir={sort_dir}"

            response = requests.get(club_url, headers=HEADERS)
            response_check(response, "circles", CLUB_SCHEMA, TOPIC)
            time.sleep(0.6)
        if rest_clubs != 0:
            club_url = f"https://uma.moe/api/v4/circles/list?page={rest_clubs}&limit={limit}&sort_by={sort_by}&sort_dir={sort_dir}"

            response = requests.get(club_url, headers=HEADERS)
            response_check(response, "circles", CLUB_SCHEMA, TOPIC)

def rank_thresholds():
    TOPIC = "rank_thresholds"

    thresholds_url = "https://uma.moe/api/v4/circles/rank-thresholds"

    response = requests.get(thresholds_url, headers=HEADERS)

    response_check(response, "thresholds", THRESHOLD_SCHEMA, TOPIC)

def response_check(response, data_key, SCHEMA, TOPIC):
    if response.status_code == 200:
        data = response.json()
        data_vals = data.get(data_key, []) # [] for default if "Circles" key is missing
        
        if not data_vals:
            print("Uma.moe API returned no records.")
            return

        sent_count = 0 # For printing how many records has been sent under the process
        for obj in data_vals: #e.g. club in circles
            sanitized_club = sanitize_json(obj, SCHEMA)
            sanitized_club["ingested_at"] = time.time()  # Add ingestion timestamp

            producer.send(TOPIC, value=sanitized_club)
            sent_count += 1

            if sent_count % 100 == 0:
                print(f"Sent {sent_count} records to Kafka.")

        producer.flush()
        print("Data sent to Kafka successfully.")

    else:
        print(f"Failed to fetch top clubs: {response.status_code} - {response.text}")

if __name__ == "__main__": # has to be launched directly and not through another script or python file
    parser = argparse.ArgumentParser(description="Data pipeline producer to Kafka. Fetches json files from the uma.moe API.")
    parser.add_argument(
        "--target", 
        choices=["clubs", "thresholds"], 
        required=True, 
        help="Use flag --target <data>. data being which pipeline producer, e.g. clubs or thresholds."
    )
    
    args = parser.parse_args() # the argument input

    if args.target == "clubs":
        uma_top_clubs()
    elif args.target == "thresholds":
        rank_thresholds()