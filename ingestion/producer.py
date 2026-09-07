import os
import json
import requests
import time
import argparse
import math
from kafka import KafkaProducer

KAFKASERVER = os.getenv("KAFKA_SERVERS", "localhost:9094")
sent_count = 0

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

MEMBER_SCHEMA = {
    "id": 0,
    "circle_id": 0,
    "viewer_id": 0,
    "trainer_name": "none",
    "shame_score": 0,
    "year": 0,
    "month": 0,
    "daily_fans": [0], # is a list, likely 30 days worth. dont be surprised at 0s if its early into the month.
    "last_updated": "1970-01-01T00:00:00Z",
    "previous_circle_id": 0,
    "previous_circle_name": "none",
    "next_month_start": 0
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

def fetch_club_members_data(circle_id):

    club_url = f"https://uma.moe/api/v4/circles?circle_id={circle_id}"

    resp = requests.get(club_url, headers=HEADERS)

    time.sleep(0.6)

    return resp

#Note: Make sure to use upsert to avoid duplicates in mongodb (idempotency) (for later)
def uma_top_clubs():
    club_topic = "uma_top_clubs"
    member_topic = "club_members"

    #Can be changed dynamically here if you wish. limit is limit per page (which is max 100.)
    sort_by = "monthly_rank"
    sort_dir = "desc"
    total_clubs = 1000
    if total_clubs <= 100:
        limit = total_clubs
        club_url = f"https://uma.moe/api/v4/circles/list?page=0&limit={limit}&sort_by={sort_by}&sort_dir={sort_dir}"

        response = requests.get(club_url, headers=HEADERS)
        clubs = response_check(response, "circles", CLUB_SCHEMA, club_topic)

        time.sleep(0.6) # sleep between API calls

        # Fetch only first 10 clubs members' data per page. This is limited due to the 1-to-N query problem, as I don't actually want to do 100 extra queries. Just going to be sampling some data.
        if total_clubs >= 10:
            for club in clubs[:10]:
                response_members = fetch_club_members_data(club["circle_id"])
                response_check(response_members, "members", MEMBER_SCHEMA, member_topic)
        elif total_clubs < 10:
            for club in range(0, total_clubs):
                response_members = fetch_club_members_data(club["circle_id"])
                response_check(response_members, "members", MEMBER_SCHEMA, member_topic)
    else:
        limit = 100
        rest_clubs = total_clubs % 100
        max_pages = math.floor(total_clubs / 100)
        for page in range(0, max_pages):
            club_url = f"https://uma.moe/api/v4/circles/list?page={page}&limit={limit}&sort_by={sort_by}&sort_dir={sort_dir}"

            response = requests.get(club_url, headers=HEADERS)
            clubs = response_check(response, "circles", CLUB_SCHEMA, club_topic)

            time.sleep(0.6)

            for club in clubs[:10]:
                response_members = fetch_club_members_data(club["circle_id"])
                response_check(response_members, "members", MEMBER_SCHEMA, member_topic)
        if rest_clubs != 0:
            club_url = f"https://uma.moe/api/v4/circles/list?page={max_pages+1}&limit={rest_clubs}&sort_by={sort_by}&sort_dir={sort_dir}"

            response = requests.get(club_url, headers=HEADERS)
            clubs = response_check(response, "circles", CLUB_SCHEMA, club_topic)

            time.sleep(0.6)

            for club in clubs[:10]:
                response_members = fetch_club_members_data(club["circle_id"])
                response_check(response_members, "members", MEMBER_SCHEMA, member_topic)

def rank_thresholds():
    threshold_topic = "rank_thresholds"

    thresholds_url = "https://uma.moe/api/v4/circles/rank-thresholds"

    response = requests.get(thresholds_url, headers=HEADERS)

    response_check(response, "thresholds", THRESHOLD_SCHEMA, threshold_topic) #response_check returns data_vals now


def response_check(response, data_key, SCHEMA, topic):
    global sent_count
    if response.status_code == 200:
        data = response.json()
        data_vals = data.get(data_key, []) # [] for default if "Circles" key is missing
        
        if not data_vals:
            print("Uma.moe API returned no records.")
            return []

        for obj in data_vals: #e.g. club in circles
            sanitized_club = sanitize_json(obj, SCHEMA)
            sanitized_club["ingested_at"] = time.time()  # Add ingestion timestamp

            producer.send(topic, value=sanitized_club)
            sent_count += 1

            if sent_count % 100 == 0:
                print(f"Sent {sent_count} records to Kafka.")

        producer.flush()
        print("Data sent to Kafka successfully.")
        return data_vals

    else:
        print(f"Failed to fetch top clubs: {response.status_code} - {response.text}")
        return []

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