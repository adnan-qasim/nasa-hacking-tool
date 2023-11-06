import requests, json, traceback
import datetime, fake_useragent
import pymongo, time, threading, os
import cc_pair_master, schedule
from pymongo.collection import ReturnDocument


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

mongo_uri = pymongo.MongoClient("mongodb://user:pass@localhost:27017/")
# mongo_uri = pymongo.MongoClient("mongodb://localhost:27017/")
dbm = mongo_uri.MasterCC
db = mongo_uri.PairsCluster


def GetPairOHLCV(
    exchange: str, pair: str, timestamp: int = int(time.time()), limit: int = 1999
):
    """ """
    print(f"getting ohlcv of {pair} in {exchange}")
    collection = db[f"{pair}"]
    db[f"{pair}"].create_index([("time", 1), ("exchange", 1)], unique=True)
    headers = {"User-Agent": fake_user_agent.chrome}
    fsym, tsym = pair.split("_")[0], pair.split("_")[1]
    url = f"https://min-api.cryptocompare.com/data/v2/histominute?fsym={fsym}&tsym={tsym}&limit={limit}&e={exchange}&toTs={timestamp-300}"
    response = requests.get(url, headers=headers).json()
    if response["Response"] == "Success":
        new = []
        for ohlcv in response["Data"]["Data"]:
            ohlcv.update(
                {
                    "time": datetime.datetime.fromtimestamp(ohlcv["time"]).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "exchange": exchange,
                }
            )
            new.append(ohlcv)
        old = list(
            collection.find({"exchange": exchange}, projection={"_id": False})
            .sort([("time", pymongo.DESCENDING)])
            .limit(2002)
        )
        writable = []
        for i in range(len(new)):
            if new[i] not in old:
                writable.append(new[i])
        if len(writable) == 0:
            return "Error"
        try:
            collection.insert_many(writable)
        except Exception:
            return "Error"
        print(f"{len(writable)} ohlcv of {pair} in {exchange} crawled")
        return [
            response["Data"]["TimeTo"],
            response["Data"]["TimeFrom"],
            len(writable),
        ]
    else:
        return response["Response"]


def GetAllExchanges(pair: str):
    startTime = datetime.datetime.now()
    doc = db.Master.find_one({"pair_sym": pair})
    total_crawled = doc["minutely_entry_count"]
    for exchange in doc["exchanges"]:
        try:
            result = GetPairOHLCV(exchange, pair)
            if result == "Error":
                continue
                # Will what to do here
            total_crawled += result[2]
            default_timestamp = datetime.datetime.fromtimestamp(result[0]).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            if doc["minutely_to_timestamp"] and len(doc["minutely_to_timestamp"]) == 19:
                try:
                    timestamp_from_doc = datetime.datetime.strptime(
                        doc["minutely_to_timestamp"], "%Y-%m-%d %H:%M:%S"
                    )
                    latest_timestamp = max(
                        timestamp_from_doc,
                        datetime.datetime.strptime(
                            default_timestamp, "%Y-%m-%d %H:%M:%S"
                        ),
                    )
                except ValueError:
                    latest_timestamp = datetime.datetime.strptime(
                        default_timestamp, "%Y-%m-%d %H:%M:%S"
                    )
            else:
                latest_timestamp = datetime.datetime.strptime(
                    default_timestamp, "%Y-%m-%d %H:%M:%S"
                )

            default_timestamp = datetime.datetime.fromtimestamp(result[1]).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            if (
                doc["minutely_from_timestamp"]
                and len(doc["minutely_from_timestamp"]) == 19
            ):
                try:
                    timestamp_from_doc = datetime.datetime.strptime(
                        doc["minutely_from_timestamp"], "%Y-%m-%d %H:%M:%S"
                    )
                    oldest_timestamp = min(
                        timestamp_from_doc,
                        datetime.datetime.strptime(
                            default_timestamp, "%Y-%m-%d %H:%M:%S"
                        ),
                    )
                except ValueError:
                    oldest_timestamp = datetime.datetime.strptime(
                        default_timestamp, "%Y-%m-%d %H:%M:%S"
                    )
            else:
                oldest_timestamp = datetime.datetime.strptime(
                    default_timestamp, "%Y-%m-%d %H:%M:%S"
                )
            doc = db.Master.find_one_and_update(
                {"pair_sym": pair},
                {
                    "$set": {
                        "minutely_to_timestamp": latest_timestamp.strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "minutely_from_timestamp": oldest_timestamp.strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                },
                return_document=ReturnDocument.AFTER,
            )
        except Exception:
            db.Master.update_one({"pair_sym": pair}, {"$pull": {"exchanges": exchange}})
            traceback_str = traceback.format_exc()
            error_info = {
                "filename": f"Crypto Compare : {pair} -> {exchange}",
                "error": traceback_str,
                "time": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            db.PairErrors.insert_one(error_info)
        finally:
            time.sleep(57)

    update = {
        "minutely_crawled_at": doc["minutely_crawled_at"],
        "minutely_entry_count": total_crawled,
        "crawler_started": startTime.strftime("%Y-%m-%d %H:%M:%S"),
        "crawler_finished": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    update["minutely_crawled_at"].append(
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    db.Master.update_one({"_id": doc["_id"]}, {"$set": update})
    print(f"{pair} Completed with time taken: {datetime.datetime.now()-startTime}")



def odd_pairs():
    """ """
    with open("./cryptocompare/pairs_list.json") as f:
        pair_list = json.load(f)
    # pair_list = ["ETH_BTC","ETH_USDT"]
    for pair in pair_list[0:22:2]:
        if pair['count'] >= 40:
            GetAllExchanges(pair['pair_sym'])


def even_pairs():
    """ """
    with open("./cryptocompare/pairs_list.json") as f:
        pair_list = json.load(f)
    # pair_list = ["BTC_USDT","LTC_BTC"]
    for pair in pair_list[1:23:2]:
        if pair['count'] >= 40:
            GetAllExchanges(pair['pair_sym'])

def schedule_functions():
    t1 = threading.Thread(target=odd_pairs)
    t2 = threading.Thread(target=even_pairs)

    # Schedule the job for odd pairs to run every day at 9 am morning.
    schedule.every().day.at("09:00").do(t1.start)

    # Schedule the job for even pairs to run every day at 9 am morning.
    schedule.every().day.at("09:00").do(t2.start)

    # Start the threads immediately 
    t1.start()
    t2.start()


# Start a new thread to run the schedule
def run_schedule():
    while True:
        schedule.run_pending()
        time.sleep(10)





#### MAIN CODE TO RUN ####

# cc_pair_master.add_master_data()
schedule_functions()
cron_thread = threading.Thread(target=run_schedule)   # Start the thread to run the schedule
cron_thread.start()
