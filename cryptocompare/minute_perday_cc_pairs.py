import requests
import json
import traceback
import datetime
import fake_useragent
import pymongo
import time
import threading
import sys
import cc_pair_master
import signal
from pymongo.collection import ReturnDocument
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

server_name = "our_server_name"

fake_user_agent = fake_useragent.FakeUserAgent()

db = cc_pair_master.db
dbs = cc_pair_master.mongo_uri.ServerLogsDB


def GetPairOHLCV(exchange: str, pair: str, limit: int = 1999):
    """Fetch OHLCV data for a given exchange and pair."""
    print(f"Getting OHLCV of {pair} in {exchange}")
    collection = db[f"{pair}"]
    headers = {"User-Agent": fake_user_agent.chrome}
    fsym, tsym = pair.split("_")[0], pair.split("_")[1]
    url = f"https://min-api.cryptocompare.com/data/v2/histominute?fsym={fsym}&tsym={tsym}&limit={limit}&e={exchange}"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {pair} on {exchange}: {e}")
        raise
    except json.JSONDecodeError:
        print(f"Failed to decode JSON response for {pair} on {exchange}")
        raise

    if data.get("Response") == "Success":
        new = []
        for ohlcv in data["Data"]["Data"]:
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
        writable = [entry for entry in new if entry not in old]

        if not writable:
            return "Error"

        try:
            collection.insert_many(writable)
            print(f"{len(writable)} OHLCV entries of {pair} in {exchange} crawled")
            return [
                data["Data"]["TimeTo"],
                data["Data"]["TimeFrom"],
                len(writable),
            ]
        except pymongo.errors.BulkWriteError as bwe:
            print(f"Bulk write error for {pair} on {exchange}: {bwe.details}")
            raise
    elif (
        data.get("Message")
        == "You are over your rate limit please upgrade your account!"
    ):
        handle_rate_limit(data)
        return GetPairOHLCV(exchange, pair, limit)
    else:
        print(f"Unexpected response for {pair} on {exchange}: {data}")
        raise Exception(data)


def handle_rate_limit(response):
    """Handle API rate limiting based on the response."""
    calls_made = response["RateLimit"]["calls_made"]
    for t, v in response["RateLimit"]["max_calls"].items():
        if calls_made.get(t, 0) > v:
            if t == "minute":
                print("Rate limit exceeded for minute. Sleeping for 60 seconds.")
                time.sleep(60)
            elif t == "hour":
                sleep_time = (65 - datetime.datetime.now().minute) * 60
                print(
                    f"Rate limit exceeded for hour. Sleeping for {sleep_time} seconds."
                )
                time.sleep(sleep_time)
            elif t == "day":
                next_day = datetime.datetime.combine(
                    datetime.date.today() + datetime.timedelta(days=1),
                    datetime.time(0, 5, 0),
                )
                sleep_seconds = (next_day - datetime.datetime.now()).total_seconds()
                print(
                    f"Rate limit exceeded for day. Sleeping until next day for {sleep_seconds} seconds."
                )
                time.sleep(sleep_seconds)
            elif t == "month":
                print("Rate limit exceeded for month. Raising exception.")
                raise Exception(response)


def GetAllExchanges(pair: str):
    """Process all exchanges for a given cryptocurrency pair."""
    startTime = datetime.datetime.now()
    doc = db.master.find_one({"pair_sym": pair})
    if not doc:
        print(f"No master document found for pair: {pair}")
        return

    total_crawled = doc.get("minutely_entry_count", 0)

    for exchange in doc.get("exchanges", []):
        try:
            result = GetPairOHLCV(exchange, pair)
            if result == "Error":
                continue
            total_crawled += result[2]

            latest_timestamp = max(
                parse_timestamp(doc.get("minutely_to_timestamp")),
                datetime.datetime.fromtimestamp(result[0]),
            )

            oldest_timestamp = min(
                parse_timestamp(doc.get("minutely_from_timestamp")),
                datetime.datetime.fromtimestamp(result[1]),
            )

            db.master.find_one_and_update(
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

        except Exception as e:
            print(f"Error processing {pair} on {exchange}: {e}")
            db.master.update_one(
                {"pair_sym": pair},
                {"$pull": {"exchanges": exchange}, "$push": {"deprecated": exchange}},
            )
            error_info = {
                "filename": f"Crypto Compare Minutely : {pair} -> {exchange}",
                "server": server_name,
                "error": traceback.format_exc(),
                "time": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            db.PairErrors.insert_one(error_info)

        finally:
            time.sleep(12)

    update = {
        "minutely_crawled_at": doc.get("minutely_crawled_at", []),
        "minutely_entry_count": total_crawled,
        "crawler_started": startTime.strftime("%Y-%m-%d %H:%M:%S"),
        "crawler_finished": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    update["minutely_crawled_at"].append(update["crawler_finished"])
    db.master.update_one({"_id": doc["_id"]}, {"$set": update})
    print(f"{pair} completed with time taken: {datetime.datetime.now() - startTime}")


def parse_timestamp(timestamp_str):
    """Parse a timestamp string to a datetime object."""
    try:
        return datetime.datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return datetime.datetime.now()


def odd_pairs():
    """Process odd-indexed cryptocurrency pairs."""
    with open("./cryptocompare/pairs_in_how_many_exchanges.json") as f:
        pair_list = json.load(f)

    # Iterate over odd-indexed pairs from the first 22 pairs in the list
    pair_list = pair_list[0:22:2]
    for pair in pair_list:
        GetAllExchanges(pair["pair_sym"])


def even_pairs():
    """Process even-indexed cryptocurrency pairs."""
    with open("./cryptocompare/pairs_in_how_many_exchanges.json") as f:
        pair_list = json.load(f)

    # Iterate over even-indexed pairs from the first 23 pairs in the list
    pair_list = pair_list[1:23:2]
    for pair in pair_list:
        GetAllExchanges(pair["pair_sym"])


def heartbeat():
    """Log heartbeat message to the database."""
    insert_data = {
        "server": server_name,
        "message": "alive",
        "time": datetime.datetime.now(),
    }
    # Remove heartbeats older than 30 days
    dbs.heartbeat.delete_many(
        {
            "server": insert_data["server"],
            "time": {"$lt": datetime.datetime.now() - datetime.timedelta(days=30)},
        }
    )
    dbs.heartbeat.insert_one(insert_data)


def schedule_functions():
    """Schedule recurring tasks using APScheduler."""
    scheduler = BackgroundScheduler()

    # Schedule the job for odd pairs to run every day at 12:00:00 PM.
    scheduler.add_job(
        odd_pairs, trigger=CronTrigger(hour=12, minute=0, second=0, day_of_week="*")
    )
    # Schedule the job for even pairs to run every day at 12:00:30 PM.
    scheduler.add_job(
        even_pairs, trigger=CronTrigger(hour=12, minute=0, second=30, day_of_week="*")
    )
    # Schedule Heartbeat to run every 15 minutes
    scheduler.add_job(heartbeat, trigger=IntervalTrigger(minutes=15))

    scheduler.start()

    def signal_handler(sig, frame):
        print("Received termination signal. Shutting down...")
        scheduler.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)


def run_schedule():
    """Keep the main thread alive to let the scheduler run."""
    while True:
        time.sleep(10)


try:
    schedule_functions()

    # Start the threads for odd_pairs and even_pairs immediately
    threading.Thread(target=odd_pairs).start()
    time.sleep(30)
    threading.Thread(target=even_pairs).start()

    run_schedule()

except Exception as e:
    traceback_str = traceback.format_exc()
    insert_data = {
        "server": server_name,
        "message": f"Code had an Error: {traceback_str}",
        "time": datetime.datetime.now(),
    }
    dbs.Errors.insert_one(insert_data)
    sys.exit(1)

# To run the script in the background:
# nohup python3 cryptocompare/minute_perday_cc_pairs.py > cc_m.txt 2>&1 &
