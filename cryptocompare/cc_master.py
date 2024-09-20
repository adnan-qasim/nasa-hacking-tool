import requests
import json
import traceback
import datetime
import fake_useragent
import pymongo
import time
import threading
import os

from pymongo import InsertOne

fake_user_agent = fake_useragent.FakeUserAgent()

# Replace with your actual MongoDB URI
mongo_uri = pymongo.MongoClient(
    "mongodb+srv://parth01:parth123@cluster0.77are8z.mongodb.net/?retryWrites=true&w=majority"
)
db = mongo_uri.MasterCC


def add_master_data():
    headers = {"User-Agent": fake_user_agent.chrome}
    url = "https://min-api.cryptocompare.com/data/v4/all/exchanges"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return
    except json.JSONDecodeError:
        print("Failed to decode JSON response")
        return

    # Ensure the directory exists
    os.makedirs("./cryptocompare", exist_ok=True)

    # Save the raw API response
    with open("./cryptocompare/abc.json", "w") as file:
        json.dump(data, file, indent=2)

    operations = []  # List to hold bulk insert operations
    filtered_list = []

    for exchange, value in data.get("Data", {}).get("exchanges", {}).items():
        if not value.get("pairs"):
            continue  # Skip exchanges with no pairs

        for pair, pair_data in value["pairs"].items():
            for tsym, info in pair_data.get("tsyms", {}).items():
                document = {
                    "exchange": exchange,
                    "parent_sym": pair,
                    "child_sym": tsym,
                    "pair_sym": f"{pair}_{tsym}",
                    "hourly_to_timestamp": "",
                    "hourly_from_timestamp": "",
                    "hourly_crawled_at": [],
                    "hourly_entry_count": 0,
                    "minutely_to_timestamp": "",
                    "minutely_from_timestamp": "",
                    "minutely_crawled_at": [],
                    "minutely_entry_count": 0,
                    "daily_to_timestamp": "",
                    "daily_from_timestamp": "",
                    "daily_crawled_at": [],
                    "daily_entry_count": 0,
                }
                document.update(info)

                # Prepare the document for insertion
                operations.append(InsertOne(document))

                # Prepare the filtered data
                filtered_data = {
                    key: value
                    for key, value in document.items()
                    if key
                    not in {
                        "hourly_to_timestamp",
                        "hourly_from_timestamp",
                        "hourly_crawled_at",
                        "hourly_entry_count",
                        "minutely_to_timestamp",
                        "minutely_from_timestamp",
                        "minutely_crawled_at",
                        "minutely_entry_count",
                        "daily_to_timestamp",
                        "daily_from_timestamp",
                        "daily_crawled_at",
                        "daily_entry_count",
                        "histo_minute_start_ts",
                        "histo_minute_start",
                        "histo_minute_end_ts",
                        "histo_minute_end",
                    }
                }
                filtered_list.append(filtered_data)

    if operations:
        try:
            result = db.master.bulk_write(operations, ordered=False)
            print(
                f"Inserted {result.inserted_count} documents into 'master' collection."
            )
        except pymongo.errors.BulkWriteError as bwe:
            print("Bulk write error occurred:")
            print(bwe.details)
        except Exception as e:
            print(f"An error occurred during bulk write: {e}")

    # Save the filtered list to JSON
    with open("./cryptocompare/pairs-list.json", "w") as file:
        json.dump(filtered_list, file, indent=4)


if __name__ == "__main__":
    add_master_data()
