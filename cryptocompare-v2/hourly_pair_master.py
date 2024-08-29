import requests, json, traceback
import datetime, fake_useragent
import pymongo, time, threading, os
import hourly_master, schedule

fake_user_agent = fake_useragent.FakeUserAgaent()

mongo_uri = pymongo.MongoClient("mongodb://gewgawrav:catax1234@concur.cumulate.live/")
# db = mongo_uri["Pairs_hourly"]
# dbc = mongo_uri["CryptoCompare_hourly"]

source_collection = mongo_uri["CryptoCompare_hourly"]["master"]
destination_collection = mongo_uri["Pairs_hourly"]["Master"]

unique_pair_syms = source_collection.distinct("pair_sym")

# Step 3: Consolidate exchange information for each pair_sym
for pair_sym in unique_pair_syms:
    # Find all documents with the same pair_sym
    documents = source_collection.find({"pair_sym": pair_sym})

    exchanges_info = {}

    for doc in documents:
        exchange_name = doc["exchange"]
        # Add exchange information to the exchanges_info dictionary
        exchanges_info[exchange_name] = {
            "histo_minute_start_ts": doc.get("histo_minute_start_ts"),
            "histo_minute_start": doc.get("histo_minute_start"),
            "histo_minute_end_ts": doc.get("histo_minute_end_ts"),
            "histo_minute_end": doc.get("histo_minute_end"),
            "isActive": doc.get("isActive", False),
        }

    # Prepare the consolidated document
    consolidated_doc = {
        "pair_sym": pair_sym,
        "exchanges": exchanges_info,
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

    # Step 4: Insert the consolidated document into the new collection
    destination_collection.insert_one(consolidated_doc)
    print(f"Consolidated data for {pair_sym} inserted.")

print("Consolidation complete.")

# def add_master_data():
#     # with open("./cryptocompare/pairs-list.json") as f:
#     #     pair_list = json.load(f)

#     pair_list = dbc["master"].find()
#     for pairs in pair_list:
#         exchanges = dbc["master"].find({"pair_sym": pairs["pair_sym"]})
#         exch_list = [exch["exchange"] for exch in exchanges]
#         pairs.update(
#             {
#                 "exchanges": exch_list,
#                 "hourly_to_timestamp": "",
#                 "hourly_from_timestamp": "",
#                 "hourly_crawled_at": [],
#                 "hourly_entry_count": 0,
#                 "minutely_to_timestamp": "",
#                 "minutely_from_timestamp": "",
#                 "minutely_crawled_at": [],
#                 "minutely_entry_count": 0,
#                 "daily_to_timestamp": "",
#                 "daily_from_timestamp": "",
#                 "daily_crawled_at": [],
#                 "daily_entry_count": 0,
#             }
#         )
#         db["Master"].insert_one(pairs)
#         print(f"Added exchanges of {pairs['pair_sym']} pair")


# if __name__ == "__main__":
#     add_master_data()
