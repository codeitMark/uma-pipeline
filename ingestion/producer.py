import os
import json
from kafka import KafkaProducer

KAFKASERVER = os.getenv("KAFKA_SERVERS", "localhost:9094")
TOPIC = "club_data"

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(value).encode('utf-8') # JSON to binary data for Kafka
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
        val = raw.json.get(key, val)

        sanitized_data[key] = def_val if val is None or val == "" else val

    return sanitized_data

