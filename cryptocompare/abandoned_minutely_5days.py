#########################################################################################################
# Coding this has been abandoned by Adnan and Gawrav sir   
# Because this shit was too complicated and logical 
# ki apni isko debug karne me hi fat ja rhi thi 
# mere ko bhi ni smj aara tha me kya kya likhe jaa rha hu
# isliye isko kabhi baad me dekhenge , we tried another simpler approach and its working makkhan 
#########################################################################################################

import requests, json, traceback
import datetime, fake_useragent
import pymongo, time, threading, os
import cc_pair_master, schedule


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
# mongo_uri = pymongo.MongoClient("mongodb://user:pass@mongodb.catax.me/")
dbm = mongo_uri.MasterCC
db = mongo_uri.PairsCluster


def GetPairOHLCV(
    exchange: str, pair: str, timestamp: int = int(time.time()), limit: int = 1999
):
    """ """
    print(f"getting ohlcv of {pair} in {exchange}")
    collection = db[f"{pair}"]
    headers = {"User-Agent": fake_user_agent.chrome}
    fsym, tsym = pair.split("_")[0], pair.split("_")[1]
    url = f"https://min-api.cryptocompare.com/data/v2/histominute?fsym={fsym}&tsym={tsym}&limit={limit}&e={exchange}&toTs={timestamp-60}"
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
        projection = {"_id": False}
        old = list(
            collection.find({"exchange": exchange}, projection)
            .sort([("time", pymongo.DESCENDING)])
            .limit(2000)
        )
        writable = []
        for dict1 in new:
            if dict1 not in old:
                writable.append(dict1)
        if len(writable) == 0:
            return "Error"
        collection.insert_many(writable)
        print(f"{len(writable)} ohlcv of {pair} in {exchange} crawled")
        return [
            response["Data"]["TimeTo"],
            response["Data"]["TimeFrom"],
            len(writable),
        ]
    else:
        return response["Response"]



def GetAllExchanges(pair: str):
    doc = db.Master.find_one({"pair_sym": pair})
    startTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    TimeStr = doc["minutely_to_timestamp"]
    total_ohlcv = doc["hourly_entry_count"]
    for exchange in doc["exchanges"]:
        # try:
        latest_to, oldest_from = (
            doc["minutely_to_timestamp"],
            doc["minutely_from_timestamp"],
        )
        timestamp = time.time()
        while True:
            result = GetPairOHLCV(exchange, pair, timestamp)
            if result == "Error":
                break
            total_ohlcv += result[2]

            default_timestamp = datetime.datetime.fromtimestamp(result[0]).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            if len(latest_to) == 19:
                try:
                    timestamp_from_pair_doc = datetime.datetime.strptime(
                        latest_to, "%Y-%m-%d %H:%M:%S"
                    )
                    latest_timestamp = max(
                        timestamp_from_pair_doc,
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
            latest_to = latest_timestamp.strftime("%Y-%m-%d %H:%M:%S")
            default_timestamp = datetime.datetime.fromtimestamp(result[1]).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            if len(oldest_from) == 19:
                try:
                    timestamp_from_pair_doc = datetime.datetime.strptime(
                        oldest_from, "%Y-%m-%d %H:%M:%S"
                    )
                    oldest_timestamp = min(
                        timestamp_from_pair_doc,
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
            oldest_from = oldest_timestamp.strftime("%Y-%m-%d %H:%M:%S")
            if doc["minutely_from_timestamp"] != "" and result[
                1
            ] < datetime.datetime.strptime(
                doc["minutely_from_timestamp"], "%Y-%m-%d %H:%M:%S"
            ).timestamp():
                break
            else:
                timestamp = result[1]

        default_timestamp = latest_to
        if len(TimeStr) == 19:
            try:
                timestamp_from_pair_doc = datetime.datetime.strptime(
                    TimeStr, "%Y-%m-%d %H:%M:%S"
                )
                old_to = min(
                    timestamp_from_pair_doc,
                    datetime.datetime.strptime(default_timestamp, "%Y-%m-%d %H:%M:%S"),
                )
            except ValueError:
                old_to = datetime.datetime.strptime(
                    default_timestamp, "%Y-%m-%d %H:%M:%S"
                )
        else:
            old_to = datetime.datetime.strptime(default_timestamp, "%Y-%m-%d %H:%M:%S")
        TimeStr = old_to.strftime("%Y-%m-%d %H:%M:%S")

        # except Exception as e:
        #     traceback_str = traceback.format_exc()
        #     errorDoc = {
        #         "pair": pair,
        #         "exchange": exchange,
        #         "error": traceback_str,
        #     }
        #     db.master.update_one({"pair_sym": pair}, {"$pull": {"exchanges": exchange}})
        #     db.PairErrors.insert_one(errorDoc)

        #     # Create a dictionary to store information about the error
        #     error_info = {
        #         "filename": f"Crypto Compare - {exchange['exchange'] }",  # A placeholder for the filename
        #         "exchange_id": exchange["_id"],
        #         "error": traceback_str,  # Record the error message and traceback
        #         "time": time.strftime(
        #             "%Y-%m-%d %H:%M:%S"
        #         ),  # Record the current date and time
        #     }

        #     # Code to send Email about error
        #     ErrorData = credentials_data
        #     ErrorData["subject"] = "Error occurred in CryptoCompare's Crawler"

        #     # Replace placeholders with actual values
        #     ErrorData[
        #         "body"
        #     ] = f"""
        #             We encountered an error in the {error_info["filename"]} data crawler system. Please find the details below:

        #             - Filename: {error_info["filename"]}
        #             - Error Time: {error_info["time"]}

        #             Error Details:

        #             {error_info["error"]}

        #             Padh liya?... Ab Jaldi jaake dekh
        #         """

        #     # mailResponse = requests.post(mailurl, json=ErrorData)

        #     # Pause execution for 5 seconds before retrying the task
        #     time.sleep(5)
        # break

    update = {
        "minutely_to_timestamp": TimeStr,
        "minutely_from_timestamp": oldest_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "minutely_crawled_at": doc["minutely_crawled_at"],
        "minutely_entry_count": total_ohlcv,
        "crawler_started":startTime,
        "crawler_finished":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    update["minutely_crawled_at"].append(
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    db.Master.update_one({"_id": doc["_id"]}, {"$set": update})


# result = GetAllExchanges("ETH_BTC")



def odd_pairs():
    """ """
    pair_list = ["ETH_BTC","ETH_USDT"]
    for pair in pair_list:
        GetAllExchanges(pair)


def even_pairs():
    """ """
    pair_list = ["BTC_USDT","LTC_BTC"]
    for pair in pair_list:
        GetAllExchanges(pair)

def schedule_functions():
    t1 = threading.Thread(target=odd_pairs)
    t2 = threading.Thread(target=even_pairs)

    # Schedule the job for odd pairs to run every 80 days at 17:30
    schedule.every(5).days.at("00:00").do(t1.start)

    # Schedule the job for even pairs to run every 80 days at 17:36
    schedule.every(5).days.at("00:00").do(t2.start)

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
