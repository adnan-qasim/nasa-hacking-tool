import requests
import time
import json
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

# Connect to Cassandra
cluster = Cluster(
    ["164.52.214.75"],
    connect_timeout=60,
    control_connection_timeout=60,
)  # Replace with your Cassandra node IP(s)
session = cluster.connect()
session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS historical_krishna WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """
)
session.set_keyspace("historical_krishna")

# File to save progress in case of stopping due to rate limits
PROGRESS_FILE = "progress_checkpoint.json"

# Define a reasonable batch size limit
BATCH_SIZE_LIMIT = 50  # Adjust the size based on the data

# Rate limits configuration
RATE_LIMITS = {
    "second": 20,
    "minute": 300,
    "hour": 1000,
    "day": 24000,
    "month": 720000,
}

# Call counters
calls_made = {
    "second": 0,
    "minute": 0,
    "hour": 0,
    "day": 0,
    "month": 0,
}
last_call_time = time.time()


# Function to create a table for a specific pair if it doesn't exist
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


# Function to insert data for a specific pair
def insert_data_for_pair(pair, data):
    insert_query = f"""
    INSERT INTO {pair} (timestamp, datetime, high, low, open, volumefrom, volumeto, close)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """
    prepared_stmt = session.prepare(insert_query)

    # Split data into chunks to prevent the batch from being too large
    for i in range(0, len(data), BATCH_SIZE_LIMIT):
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        chunk = data[i : i + BATCH_SIZE_LIMIT]  # Create a batch-sized chunk of data

        for record in chunk:
            timestamp = record["time"]
            dt = datetime.utcfromtimestamp(timestamp)  # Convert Unix time to datetime
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

        session.execute(batch)  # Execute the batch after preparing it
        print(f"Inserted {len(chunk)} records into {pair}.")


# Function to fetch hourly data for a pair from the API
def fetch_hourly_data(fsym, tsym, to_timestamp):
    url = "https://min-api.cryptocompare.com/data/v2/histohour"
    params = {
        "fsym": fsym,
        "tsym": tsym,
        "limit": 2000,  # Max data points per request
        "toTs": to_timestamp,
        "e": "CCCAGG",
    }
    response = requests.get(url, params=params)

    if response.status_code == 200:
        print(
            f"Fetched data for {fsym}/{tsym} until {datetime.utcfromtimestamp(to_timestamp).strftime('%Y-%m-%d %H:%M:%S')}."
        )
        return response.json()
    else:
        print(
            f"Failed to fetch data for {fsym}/{tsym}: {response.status_code} - {response.text}"
        )
        return None


# Function to handle API rate limits based on calls made
import time


# Function to handle API rate limits based on calls made with optimized sleep times
def handle_rate_limits():
    global calls_made, last_call_time

    current_time = time.time()
    current_datetime = datetime.now()

    # Reset the call counters based on time passed since last call
    calls_made["second"] += 1
    calls_made["minute"] += 1
    calls_made["hour"] += 1
    calls_made["day"] += 1
    calls_made["month"] += 1

    # Calculate time until the next second, minute, hour, and day
    next_second = (current_datetime.second + 1) % 60
    next_minute = (current_datetime.minute + 1) % 60
    next_hour = (current_datetime.hour + 1) % 24
    next_day = current_datetime.day + 1

    # Calculate remaining time until the next reset
    time_until_next_second = max(1 - (current_time % 1), 0)
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

    # Check the rate limits
    if calls_made["second"] >= RATE_LIMITS["second"]:
        print("Reached per-second limit. Sleeping until next second...")
        time.sleep(time_until_next_second)
        calls_made["second"] = 0  # Reset after sleeping

    if calls_made["minute"] >= RATE_LIMITS["minute"]:
        print("Reached per-minute limit. Sleeping until next minute...")
        time.sleep(time_until_next_minute)
        calls_made["minute"] = 0  # Reset after sleeping

    if calls_made["hour"] >= RATE_LIMITS["hour"]:
        print("Reached per-hour limit. Sleeping until next hour...")
        time.sleep(time_until_next_hour)
        calls_made["hour"] = 0  # Reset after sleeping

    if calls_made["day"] >= RATE_LIMITS["day"]:
        print("Reached per-day limit. Sleeping until next day...")
        time.sleep(time_until_next_day)
        calls_made["day"] = 0  # Reset after sleeping

    last_call_time = current_time  # Update last call time


# Save the current progress in a JSON file
def save_progress(pair, timestamp):
    progress_data = {"pair": pair, "timestamp": timestamp}
    with open(PROGRESS_FILE, "w") as file:
        json.dump(progress_data, file)
    print(f"Progress saved for pair: {pair}, timestamp: {timestamp}")


# Load progress from the JSON file
def load_progress():
    try:
        with open(PROGRESS_FILE, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        return None


# Main function to handle all pairs
def main():
    # Load pair data from sorted_pair_exchanges.json
    with open("sorted_pair_exchanges.json", "r") as f:
        pairs_data = json.load(f)

    # Load progress to resume from where we left off
    progress = load_progress()
    start_pair = progress.get("pair") if progress else None
    start_timestamp = progress.get("timestamp") if progress else None

    print("Starting data fetching process...")
    print(f"Resuming from pair: {start_pair}, timestamp: {start_timestamp}")

    # Start processing pairs
    for pair, exchanges in pairs_data.items():
        if start_pair and pair < start_pair:
            print(f"Skipping pair {pair}, already processed before checkpoint.")
            continue  # Skip pairs already processed before checkpoint

        fsym, tsym = pair.split("_")

        # Create the table for the pair if it doesn't exist
        create_table_for_pair(pair)

        # Get the current timestamp and fetch data from current time back to 2017
        end_timestamp = start_timestamp if start_timestamp else int(time.time())
        start_timestamp = int(datetime(2017, 1, 1).timestamp())  # Start of 2017

        while end_timestamp > start_timestamp:
            print(
                f"Fetching data for {pair} until {datetime.utcfromtimestamp(end_timestamp).strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # Fetch data for the pair
            response_json = fetch_hourly_data(fsym, tsym, end_timestamp)
            if response_json is None:
                print(f"No data returned for {pair}. Exiting fetch loop.")
                break

            # Get the data array
            data = response_json.get("Data", {}).get("Data", [])
            if not data:
                print(f"No data available for {pair}. Exiting fetch loop.")
                break

            # Insert the data into Cassandra
            insert_data_for_pair(pair, data)

            # Update the end timestamp to the earliest time from the fetched data
            end_timestamp = data[0]["time"]

            # Save progress in case the rate limit is reached or the script is interrupted
            save_progress(pair, end_timestamp)

            # Handle API rate limits based on the response
            handle_rate_limits()

    print("Data fetching and insertion complete.")


if __name__ == "__main__":
    main()
