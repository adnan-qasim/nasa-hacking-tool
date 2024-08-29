import requests, json, traceback
import datetime, fake_useragent
import time, threading, os

from pymongo import MongoClient

fake_user_agent = fake_useragent.FakeUserAgent()

mongo_uri = MongoClient("mongodb://gewgawrav:catax1234@concur.cumulate.live/")
db = mongo_uri["CryptoCompare_hourly"]

fake_user_agent = fake_useragent.FakeUserAgent()


def add_master_data():
    headers = {"User-Agent": fake_user_agent.chrome}
    url = "https://min-api.cryptocompare.com/data/v4/all/exchanges"
    response = requests.get(url, headers=headers).json()
    filtered_list = []
    for exchange, value in response["Data"]["exchanges"].items():
        collection = db[f"master"]
        if value["pairs"] != {}:
            for pair, data in value.get("pairs", {}).items():
                for tsym, info in data.get("tsyms", {}).items():
                    insert = {
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
                    insert.update(info)
                    m_ = collection.find_one(
                        {
                            "exchange": exchange,
                            "parent_sym": pair,
                            "child_sym": tsym,
                        }
                    )
                    if m_ == None:
                        id1 = collection.insert_one(insert).inserted_id
                        print(f"exchange {exchange} data inserted ")

                    else:
                        print(f"exchange {exchange} data exists ")
                        insert = m_
                    fields_to_remove = [
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
                    ]

                    # Use a dictionary comprehension to create a new dictionary with specified fields removed
                    filtered_data = {
                        key: value
                        for key, value in insert.items()
                        if key not in fields_to_remove
                    }
                    filtered_data["_id"] = str(filtered_data["_id"])
                    filtered_list.append(filtered_data)
    with open("./cryptocompare-v2/pairs-list.json", "w") as file:  # 212400
        json.dump(filtered_list, file, indent=4)


if __name__ == "__main__":
    add_master_data()
