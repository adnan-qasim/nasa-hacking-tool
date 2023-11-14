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

db = cc_pair_master.db


def GetPairOHLCV(
    exchange: str, pair: str, timestamp: int = int(time.time()), limit: int = 1999
):
    """ """
    print(f"getting ohlcv of {pair} in {exchange}")
    collection = db[f"{pair}"]
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
        return GetPairOHLCV(exchange, fsym, tsym, limit, timestamp)

    else:
        raise Exception(response)


def GetAllExchanges(pair: str):
    startTime = datetime.datetime.now()
    doc = db.master.find_one({"pair_sym": pair})
    total_crawled = doc["minutely_entry_count"]
    for exchange in doc["exchanges"]:
        try:
            result = GetPairOHLCV(exchange, pair)
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
            db.master.update_one({"pair_sym": pair}, {"$pull": {"exchanges": exchange}})
            traceback_str = traceback.format_exc()
            error_info = {
                "filename": f"Crypto Compare Minutely : {pair} -> {exchange}",
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
                    - Error Time: {error_info["time"]}

                    Error Details:

                    {error_info["error"]}
                    
                    Padh liya?... Ab Jaldi jaake dekh
                """

            mailResponse = requests.post(mailurl, json=ErrorData)
            ErrorData["destination_email"] = "shiekh111aq@gmail.com"
            mailResponse = requests.post(mailurl, json=ErrorData)

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
    # Opening the JSON file to read the list of pairs
    with open("./cryptocompare/pairs_list.json") as f:
        pair_list = json.load(f)

    # Iterate over odd indexed pairs from the first 22 pairs in the list
    for pair in pair_list[0:22:2]:
        # Check if the 'count' of the pair is 40 or more
        if pair["count"] >= 40:
            # If yes, then get all exchanges for this pair
            GetAllExchanges(pair["pair_sym"])

    # Prepare the email data using the credentials_data variable
    mail_data = credentials_data
    mail_data[
        "subject"
    ] = "Badhai Ho!! aaj ka minutely crawler ka odd index pairs khatam.."
    mail_data[
        "body"
    ] = f""" 
        Name: Minutely Odd Index
        Server IP: 
        Exchanges Completed: {pair_list[0:22:2]}
        Completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}
        
        Padh liya... ab jaake usko aaghe ka kaam de 😂
    """

    # Send the email using a POST request to the mailurl
    mailResponse = requests.post(mailurl, json=mail_data)
    # Update the recipient email
    mail_data["destination_email"] = "shiekh111aq@gmail.com"
    # Send the email again to the updated recipient
    mailResponse = requests.post(mailurl, json=mail_data)


def even_pairs():
    """
    This function processes even indexed cryptocurrency pairs from a JSON file,
    checks if their count is above a threshold, fetches data for eligible pairs,
    and then sends an email notification about the completion of the task.
    """
    # Opening the JSON file to read the list of pairs
    with open("./cryptocompare/pairs_list.json") as f:
        pair_list = json.load(f)

    # Iterate over even indexed pairs from the first 23 pairs in the list
    for pair in pair_list[1:23:2]:
        # Check if the 'count' of the pair is 40 or more
        if pair["count"] >= 40:
            # If yes, then get all exchanges for this pair
            GetAllExchanges(pair["pair_sym"])

    # Prepare the email data using the credentials_data variable
    mail_data = credentials_data
    mail_data[
        "subject"
    ] = "Badhai Ho!! aaj ka minutely crawler ka even index pairs khatam.."
    mail_data[
        "body"
    ] = f"""
        Name: Minutely Even Index 
        Server IP: 
        Exchanges Completed: {pair_list[1:23:2]}
        Completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}
        
        Padh liya... ab jaake usko aaghe ka kaam de 😂
    """

    # Send the email using a POST request to the mailurl
    mailResponse = requests.post(mailurl, json=mail_data)
    # Update the recipient email
    mail_data["destination_email"] = "shiekh111aq@gmail.com"
    # Send the email again to the updated recipient
    mailResponse = requests.post(mailurl, json=mail_data)


def schedule_functions():
    t1 = threading.Thread(target=odd_pairs)
    t2 = threading.Thread(target=even_pairs)

    # Schedule the job for odd pairs to run every day at 9 am morning.
    schedule.every().day.at("09:00:00").do(t1.start)

    # Schedule the job for even pairs to run every day at 9 am morning.
    schedule.every().day.at("09:00:30").do(t2.start)

    # Start the threads immediately
    t1.start()
    time.sleep(30)
    t2.start()


# Start a new thread to run the schedule
def run_schedule():
    while True:
        schedule.run_pending()
        time.sleep(10)


#### MAIN CODE TO RUN ####

# cc_pair_master.add_master_data()
schedule_functions()
cron_thread = threading.Thread(
    target=run_schedule
)  # Start the thread to run the schedule
cron_thread.start()
