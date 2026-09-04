import os
import json
import requests
import time
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
    "circle_id": None,
    "name": "Unknown",
    "comment": "No comment",
    "member_count": 0,
    "created_at": "1970-01-01T00:00:00Z",
    "updated_at": "1970-01-01T00:00:00Z",
    "monthly_rank": 0,
    "monthly_point": 0,
    "last_month_rank": 0,
    "last_month_point": 0,
    "live_points": 0,
    "live_rank": 0,
    "last_live_update": "1970-01-01T00:00:00Z",
    "club_rank": 0
} #club_rank 11 is SS, 10 is S+, 9 is S, etc.

# sanitizing club data to stream only required fields above, and fill missing fields with default values
def sanitize_json(raw_json, schema):
    # Checks if the input is a json object (dict)
    # If it is not, returns a new dict with default values from the schema
    if not isinstance(raw_json, dict):
        return {key: default for key, default in schema.items()}

    sanitized_data = {}

    for key, def_val in schema.items():
        val = raw_json.get(key, def_val)

        sanitized_data[key] = def_val if val is None or val == "" else val

    return sanitized_data

#Note: Make sure to use upsert to avoid duplicates in mongodb (idempotency) (for later)
def uma_top_clubs():
    TOPIC = "uma_top_clubs"

    #Can be changed dynamically here if you wish.
    limit = 10000
    sort_by = "monthly_rank"
    sort_dir = "desc"

    url = f"https://uma.moe/api/v4/circles/list?page=0&limit={limit}&sort_by={sort_by}&sort_dir={sort_dir}"

    response = requests.get(url, headers=HEADERS)

    if response.status_code == 200:
        clubs_data = response.json()
        circles = clubs_data.get("circles", []) # [] for default if "Circles" key is missing
        
        if not circles:
            print("Uma.moe API returned no club records.")
            return

        sent_count = 0 # For printing how many records has been sent under the process
        for club in circles:
            sanitized_club = sanitize_json(club, CLUB_SCHEMA)
            sanitized_club["ingested_at"] = time.time()  # Add ingestion timestamp

            producer.send('top_clubs', value=sanitized_club)
            sent_count += 1

            if sent_count % 1000 == 0:
                print(f"Sent {sent_count} club records to Kafka.")
                
        producer.flush()
        print("Clubs data sent to Kafka successfully.")

    else:
        print(f"Failed to fetch top clubs: {response.status_code} - {response.text}")