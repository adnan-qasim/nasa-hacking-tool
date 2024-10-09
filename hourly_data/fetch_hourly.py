import requests
import time
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from pymongo import MongoClient
import json
import sys

# Cassandra connection
cluster = Cluster(
    ["164.52.214.75"],
    connect_timeout=60,
    control_connection_timeout=60,
)
session = cluster.connect()
session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS historical_krishna 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
"""
)
session.set_keyspace("historical_krishna")

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client["progress_tracker"]
progress_collection = db["progress"]

BATCH_SIZE_LIMIT = 50
RATE_LIMITS = {
    "second": 20,
    "minute": 300,
    "hour": 1000,
    "day": 24000,
    "month": 720000,
}

calls_made = {"second": 0, "minute": 0, "hour": 0, "day": 0, "month": 0}
last_call_time = time.time()

# Command-line argument for server name
server_name = sys.argv[1] if len(sys.argv) > 1 else "server_1"
start_index = int(sys.argv[2]) if len(sys.argv) > 2 else 0
end_index = int(sys.argv[3]) if len(sys.argv) > 3 else None


def create_table_for_pair(pair):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {pair} (
        timestamp bigint PRIMARY KEY,
        datetime timestamp,
        high double,
        low double,
        open double,
        volumefrom double,
        volumeto double,
        close double
    );
    """
    session.execute(create_table_query)
    print(f"Table created (or already exists) for pair: {pair}")


def insert_data_for_pair(pair, data):
    insert_query = f"""
    INSERT INTO {pair} (timestamp, datetime, high, low, open, volumefrom, volumeto, close)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """
    prepared_stmt = session.prepare(insert_query)

    for i in range(0, len(data), BATCH_SIZE_LIMIT):
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        chunk = data[i : i + BATCH_SIZE_LIMIT]
        for record in chunk:
            timestamp = record["time"]
            dt = datetime.utcfromtimestamp(timestamp)
            high = record["high"]
            low = record["low"]
            open_val = record["open"]
            close = record["close"]
            volumefrom = record["volumefrom"]
            volumeto = record["volumeto"]

            batch.add(
                prepared_stmt,
                (timestamp, dt, high, low, open_val, volumefrom, volumeto, close),
            )

        session.execute(batch)
        print(f"Inserted {len(chunk)} records into {pair}.")


def fetch_hourly_data(fsym, tsym, to_timestamp):
    url = "https://min-api.cryptocompare.com/data/v2/histohour"
    params = {
        "fsym": fsym,
        "tsym": tsym,
        "limit": 2000,
        "toTs": to_timestamp,
        "e": "CCCAGG",
    }
    response = requests.get(url, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(
            f"Error fetching data for {fsym}/{tsym}: {response.status_code} - {response.text}"
        )
        return None


def handle_rate_limits():
    global calls_made, last_call_time
    current_time = time.time()
    current_datetime = datetime.now()

    calls_made["minute"] += 1
    calls_made["hour"] += 1
    calls_made["day"] += 1

    time_until_next_minute = max(60 - current_datetime.second, 0)
    time_until_next_hour = max(
        3600 - (current_datetime.minute * 60 + current_datetime.second), 0
    )
    time_until_next_day = max(
        86400
        - (
            current_datetime.hour * 3600
            + current_datetime.minute * 60
            + current_datetime.second
        ),
        0,
    )

    if calls_made["minute"] >= RATE_LIMITS["minute"]:
        print("Rate limit reached for minute. Sleeping...")
        time.sleep(time_until_next_minute)
        calls_made["minute"] = 0

    if calls_made["hour"] >= RATE_LIMITS["hour"]:
        print("Rate limit reached for hour. Sleeping...")
        time.sleep(time_until_next_hour)
        calls_made["hour"] = 0

    if calls_made["day"] >= RATE_LIMITS["day"]:
        print("Rate limit reached for day. Sleeping...")
        time.sleep(time_until_next_day)
        calls_made["day"] = 0

    last_call_time = current_time


def save_progress(pair, timestamp, pair_index):
    progress_data = {
        "server": server_name,
        "status": "running",
        "pair": pair,
        "timestamp": timestamp,
        "pair_index": pair_index,
        "last_saved": datetime.now(),
    }
    progress_collection.update_one(
        {"server": server_name}, {"$set": progress_data}, upsert=True
    )
    print(f"Progress saved: {pair}, timestamp: {timestamp}, pair_index: {pair_index}")


def load_progress():
    return progress_collection.find_one({"server": server_name}, sort=[("last_saved", -1)])


def main():
    with open("sorted_pair_exchanges.json", "r") as f:
        pairs_data = json.load(f)

    progress = load_progress()
    start_pair = progress["pair"] if progress else None
    start_timestamp = progress["timestamp"] if progress else None
    pair_index = progress["pair_index"] if progress else start_index

    print(
        f"Resuming from: {start_pair}, Timestamp: {start_timestamp}, Index: {pair_index} (Server: {server_name})"
    )

    for index, (pair, exchanges) in enumerate(pairs_data.items()):
        if index < pair_index or (end_index is not None and index > end_index):
            continue

        fsym, tsym = pair.split("_")
        create_table_for_pair(pair)

        end_timestamp = start_timestamp if start_timestamp else int(time.time())
        start_timestamp = int(datetime(2023, 1, 1).timestamp())

        while end_timestamp > start_timestamp:
            response_json = fetch_hourly_data(fsym, tsym, end_timestamp)
            if not response_json:
                break

            data = response_json.get("Data", {}).get("Data", [])
            if not data:
                break

            insert_data_for_pair(pair, data)
            end_timestamp = data[0]["time"]

            save_progress(pair, end_timestamp, index)
            handle_rate_limits()

        start_timestamp = None  # Reset for the next pair

    # Mark server progress as complete
    progress_collection.update_one(
        {"server": server_name}, {"$set": {"status": "stopped"}}, upsert=True
    )
    print("Data fetching completed.")


if __name__ == "__main__":
    main()
