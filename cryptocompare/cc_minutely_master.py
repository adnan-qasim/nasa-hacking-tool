import json
import fake_useragent
import pymongo
import cc_hourly_master as cc_hourly_master
from env import *





fake_user_agent = fake_useragent.FakeUserAgent()

# mongo_uri = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_uri = pymongo.MongoClient(f"mongodb://{mongo_user_pass}@tongodb.catax.me/", port=27018)
mongo_uri2 = pymongo.MongoClient(f"mongodb://{mongo_user_pass}@chongodb.catax.me/")
dbm = mongo_uri2.MasterCC
db = mongo_uri.MinutelyCC


def add_master_data():
    with open("./cryptocompare/pairs_list_for_minutely.json") as f:
        pair_list = json.load(f)
    for pairs in pair_list:
        exchanges = dbm.master.find({"pair_sym": pairs["pair_sym"]})
        exch_list = [exch["exchange"] for exch in exchanges]
        pairs.update(
            {
                "exchanges": exch_list,
                "count": len(exch_list),
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
        )
        db.master.insert_one(pairs)

        print(f"Added exchanges of {pairs['pair_sym']} pair")


if __name__ == "__main__":
    add_master_data()
