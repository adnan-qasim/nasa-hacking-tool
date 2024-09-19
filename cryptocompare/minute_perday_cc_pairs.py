import requests, json, traceback
import datetime, fake_useragent
import pymongo, time, threading, os
import cc_pair_master, signal, sys
from pymongo.collection import ReturnDocument
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger


mailurl = "https://emailsender.catax.me/sendEmail"
server_name = "Server-1"


credentials_data = {
    "username": "AKIAVG3KVGIQ5K5C54EV",
    "password": "BGI30r7ViaHz5pMhtMjkqw/GDeAD4S3McLoMJltIaaqF",
    "server_addr": "email-smtp.eu-north-1.amazonaws.com",
    "server_port": "587",
    "destination_email": "shiekh111aq@gmail.com",
    "sender_email": "error@catax.me",
    "subject": "Test Email",
    "body": "This is a test email. Hello from Error!",
}
last_mail = datetime.datetime.now() - datetime.timedelta(minutes=30)


fake_user_agent = fake_useragent.FakeUserAgent()

db = cc_pair_master.db
dbs = cc_pair_master.mongo_uri.ServerLogsDB


def GetPairOHLCV(exchange: str, pair: str, limit: int = 1999):
    """ """
    print(f"getting ohlcv of {pair} in {exchange}")
    collection = db[f"{pair}"]
    headers = {"User-Agent": fake_user_agent.chrome}
    fsym, tsym = pair.split("_")[0], pair.split("_")[1]
    url = f"https://min-api.cryptocompare.com/data/v2/histominute?fsym={fsym}&tsym={tsym}&limit={limit}&e={exchange}"
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
        collection.insert_many(writable)
        print(f"{len(writable)} ohlcv of {pair} in {exchange} crawled")
        return [
            response["Data"]["TimeTo"],
            response["Data"]["TimeFrom"],
            len(writable),
        ]
    elif (
        response["Message"]
        == "You are over your rate limit please upgrade your account!"
    ):
        calls_made = response["RateLimit"]["calls_made"]
        for t, v in response["RateLimit"]["max_calls"].items():
            if calls_made[t] > v:
                if t == "minute":
                    time.sleep(60)
                elif t == "hour":
                    time.sleep((65 - datetime.datetime.now().minute) * 60)
                elif t == "day":
                    time.sleep(
                        (
                            datetime.datetime.combine(
                                datetime.date.today() + datetime.timedelta(days=1),
                                datetime.time(0, 5, 0),
                            )
                            - datetime.datetime.now()
                        ).total_seconds()
                    )
                elif t == "month":
                    raise Exception(response)
        return GetPairOHLCV(exchange, fsym, tsym, limit)

    else:
        raise Exception(response)


def GetAllExchanges(pair: str):
    startTime = datetime.datetime.now()
    doc = db.master.find_one({"pair_sym": pair})
    total_crawled = doc["minutely_entry_count"]
    for exchange in doc["exchanges"]:
        try:
            result = GetPairOHLCV(exchange, pair)
            if result == "Error":
                continue
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
            doc = db.master.find_one_and_update(
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
            db.master.update_one(
                {"pair_sym": pair},
                {"$pull": {"exchanges": exchange}, "$push": {"deprecated": exchange}},
            )
            traceback_str = traceback.format_exc()
            error_info = {
                "filename": f"Crypto Compare Minutely : {pair} -> {exchange}",
                "server": server_name,
                "error": traceback_str,
                "time": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            db.PairErrors.insert_one(error_info)

            # Code to send Email about error
            ErrorData = credentials_data
            ErrorData["subject"] = "Error occurred in CryptoCompare's Crawler"

            # Replace placeholders with actual values
            ErrorData[
                "body"
            ] = f"""
                    We encountered an error in the {error_info["filename"]} data crawler system. Please find the details below:

                    - Filename: {error_info["filename"]}
                    - Server: {server_name}
                    - Error Time: {error_info["time"]}

                    Error Details:

                    {error_info["error"]}
                    
                    Padh liya?... Ab Jaldi jaake dekh
                """

            global last_mail
            if datetime.datetime.now() >= last_mail + datetime.timedelta(minutes=30):
                mailResponse = requests.post(mailurl, json=ErrorData)
                ErrorData["destination_email"] = "shiekh111aq@gmail.com"   
                mailResponse = requests.post(mailurl, json=ErrorData)
                last_mail = datetime.datetime.now()

        finally:
            time.sleep(12)

    update = {
        "minutely_crawled_at": doc["minutely_crawled_at"],
        "minutely_entry_count": total_crawled,
        "crawler_started": startTime.strftime("%Y-%m-%d %H:%M:%S"),
        "crawler_finished": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    update["minutely_crawled_at"].append(
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    db.master.update_one({"_id": doc["_id"]}, {"$set": update})
    print(f"{pair} Completed with time taken: {datetime.datetime.now()-startTime}")


def odd_pairs():
    """
    This function processes odd indexed cryptocurrency pairs from a JSON file,
    checks if their count is above a threshold, fetches data for eligible pairs,
    and then sends an email notification about the completion of the task.
    """
    global last_mail
    # Opening the JSON file to read the list of pairs
    with open("./cryptocompare/pairs_list.json") as f:
        pair_list = json.load(f)

    # Iterate over odd indexed pairs from the first 22 pairs in the list
    pair_list = pair_list[0:22:2]
    for pair in pair_list:
        GetAllExchanges(pair["pair_sym"])

    # Prepare the email data using the credentials_data variable
    mail_data = credentials_data
    mail_data["sender_email"] = "badhai@catax.me"
    mail_data["subject"] = (
        "Badhai Ho!! aaj ka minutely crawler ka odd index pairs khatam.."
    )
    mail_data[
        "body"
    ] = f"""
        Name: Minutely Odd Index
        Server: {server_name}
        Exchanges Completed: {pair_list}
        Completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}

        Padh liya... AB SOO JA 😂
    """

    # Send the email using a POST request to the mailurl
    if datetime.datetime.now() >= last_mail + datetime.timedelta(minutes=30):
        mailResponse = requests.post(mailurl, json=mail_data)
        last_mail = datetime.datetime.now()


def even_pairs():
    """
    This function processes even indexed cryptocurrency pairs from a JSON file,
    checks if their count is above a threshold, fetches data for eligible pairs,
    and then sends an email notification about the completion of the task.
    """
    global last_mail
    # Opening the JSON file to read the list of pairs
    with open("./cryptocompare/pairs_list.json") as f:
        pair_list = json.load(f)

    # Iterate over even indexed pairs from the first 23 pairs in the list
    pair_list = pair_list[1:23:2]
    for pair in pair_list:
        GetAllExchanges(pair["pair_sym"])

    # Prepare the email data using the credentials_data variable
    mail_data = credentials_data
    mail_data["sender_email"] = "badhai@catax.me"
    mail_data["subject"] = (
        "Badhai Ho!! aaj ka minutely crawler ka even index pairs khatam.."
    )
    mail_data[
        "body"
    ] = f"""
        Name: Minutely Even Index
        Server: {server_name}
        Exchanges Completed: {pair_list}
        Completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}

        Padh liya... AB SOO JA 😂
    """

    # Send the email using a POST request to the mailurl
    if datetime.datetime.now() >= last_mail + datetime.timedelta(minutes=30):
        mailResponse = requests.post(mailurl, json=mail_data)
        last_mail = datetime.datetime.now()


def heartbeat():
    insert_data = {
        "server": server_name,
        "message": "alive",
        "time": datetime.datetime.now(),
    }
    dbs.heartbeat.delete_many(
        {
            "server": insert_data["server"],
            "time": {"$lt": datetime.datetime.now() - datetime.timedelta(days=30)},
        }
    )
    dbs.heartbeat.insert_one(insert_data)


def schedule_functions():
    scheduler = BackgroundScheduler()
    # Schedule the job for odd pairs to run every day at 2:30:00 AM.
    scheduler.add_job(
        odd_pairs, trigger=CronTrigger(hour=12, minute=0, second=0, day_of_week="*")
    )
    # Schedule the job for even pairs to run every day at 2:30:30 AM.
    scheduler.add_job(
        even_pairs, trigger=CronTrigger(hour=12, minute=0, second=30, day_of_week="*")
    )
    # Triggers Heartbeat after every 15 minutes
    scheduler.add_job(heartbeat, trigger=IntervalTrigger(minutes=15))
    # Start the scheduler
    scheduler.start()

    def signal_handler(signal, frame):
        print("Received termination signal. Shutting down...")
        scheduler.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)


def run_schedule():
    while True:
        time.sleep(10)


try:
    # cc_pair_master.add_master_data()
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


# nohup python3 cryptocompare/minute_perday_cc_pairs.py > cc_m.txt 2>&1 &
