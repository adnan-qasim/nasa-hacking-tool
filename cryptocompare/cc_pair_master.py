import requests, json, traceback
import datetime, fake_useragent
import pymongo, time, threading, os
import cc_master, schedule


mailurl = "https://emailsender.catax.me/sendEmail"


credentials_data = {
    "username": "AKIAVG3KVGIQ5K5C54EV",
    "password": "BGI30r7ViaHz5pMhtMjkqw/GDeAD4S3McLoMJltIaaqF",
    "server_addr": "email-smtp.eu-north-1.amazonaws.com",
    "server_port": "587",
    "destination_email": "gewgawrav@gmail.com",
    "sender_email": "error@catax.me",
    "subject": "Test Email",
    "body": "This is a test email. Hello from Error!",
}

fake_user_agent = fake_useragent.FakeUserAgent()

mongo_uri = pymongo.MongoClient("mongodb://localhost:27017/")
# mongo_uri = pymongo.MongoClient("mongodb://user:pass@localhost:27017/")
mongo_uri2 = pymongo.MongoClient("mongodb://user:pass@mongodb.catax.me/")
dbm = mongo_uri2.MasterCC
db = mongo_uri.PairsCluster


def add_master_data():
    with open("./cryptocompare/pairs_list.json") as f:
        pair_list = json.load(f)
    for pairs in pair_list:
        exchanges = dbm.master.find({"pair_sym": pairs["pair_sym"]})
        exch_list = [exch["exchange"] for exch in exchanges]
        pairs.update(
            {
                "exchanges": exch_list,
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
