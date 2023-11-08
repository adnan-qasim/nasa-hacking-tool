import requests, json, traceback
import datetime, fake_useragent, cc_master
import pymongo, time, threading, os
from bson import ObjectId

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

# mongo_uri = pymongo.MongoClient("mongodb://localhost:27017/")
db = cc_master.mongo_uri.MasterCC


def GetPairOHLCV(
    exchange: str, fsym: str, tsym: str, limit: int, timestamp: int = int(time.time())
):
    """ """
    collection = db[f"{exchange}"]
    headers = {"User-Agent": fake_user_agent.chrome}
    url = f"https://min-api.cryptocompare.com/data/v2/histohour?fsym={fsym}&tsym={tsym}&limit={limit}&e={exchange}&toTs={((timestamp//3600)*3600)-3600}"
    response = requests.get(url, headers=headers).json()
    if response["Response"] == "Success":
        new = []
        for ohlcv in response["Data"]["Data"]:
            ohlcv.update(
                {
                    "time": datetime.datetime.fromtimestamp(ohlcv["time"]).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "timestamp": ohlcv["time"],
                    "parent_sym": fsym,
                    "child_sym": tsym,
                }
            )
            new.append(ohlcv)
        try:
            collection.insert_many(new)
        except Exception as e:
            raise Exception(
                f"There was an error inserting many documents into db of {fsym}_{tsym} \n error:{e}"
            )
        print(f"{len(new)} ohlcv of {exchange}'s {fsym}_{tsym} crawled")
        return [
            response["Data"]["TimeTo"],
            response["Data"]["TimeFrom"],
            len(new),
        ]
    else:
        raise Exception(response)


def CallAllPairs(exchange_id: str):
    """ """
    while True:
        pair_doc = db.master.find_one({"_id": ObjectId(exchange_id)})
        if pair_doc is None:
            raise Exception(f"There are No Documents with Exchange id = {exchange_id}")
        exchange = pair_doc["exchange"]
        fsym = pair_doc["parent_sym"]
        tsym = pair_doc["child_sym"]
        # if len(pair_doc["hourly_crawled_at"]) > 0:
        #     time_difference = datetime.datetime.now() - datetime.datetime.strptime(
        #         pair_doc["hourly_crawled_at"][-1], "%Y-%m-%d %H:%M:%S"
        #     )
        #     if time_difference.total_seconds() < 30 * 24 * 3600:  # 30 days in seconds
        #         print(
        #             f"{exchange}'s {pair_doc['pair_sym']} was just crawled {int(time_difference.total_seconds() / (24 * 3600))} days ago"
        #         )
        #         return None

        print("working")
        toTs = (
            pair_doc.get("hourly_from_ts")
            if pair_doc.get("hourly_from_ts", False)
            else int(time.time())
        )
        crawled = GetPairOHLCV(exchange, fsym, tsym, 1999, toTs)
        if crawled != None:
            default_timestamp = datetime.datetime.fromtimestamp(crawled[0]).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            if (
                pair_doc["hourly_to_timestamp"]
                and len(pair_doc["hourly_to_timestamp"]) == 19
            ):
                try:
                    timestamp_from_pair_doc = datetime.datetime.strptime(
                        pair_doc["hourly_to_timestamp"], "%Y-%m-%d %H:%M:%S"
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

            default_timestamp = datetime.datetime.fromtimestamp(crawled[1]).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            if (
                pair_doc["hourly_from_timestamp"]
                and len(pair_doc["hourly_from_timestamp"]) == 19
            ):
                try:
                    timestamp_from_pair_doc = datetime.datetime.strptime(
                        pair_doc["hourly_from_timestamp"], "%Y-%m-%d %H:%M:%S"
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

            update = {
                "hourly_to_timestamp": latest_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "hourly_from_timestamp": oldest_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "hourly_to_ts": latest_timestamp.timestamp(),
                "hourly_from_ts": oldest_timestamp.timestamp(),
                # "hourly_crawled_at": pair_doc["hourly_crawled_at"],
                "hourly_entry_count": pair_doc["hourly_entry_count"] + crawled[2],
            }
            db.master.find_one_and_update({"_id": pair_doc["_id"]}, {"$set": update})
            if pair_doc["histo_minute_start_ts"] > update["hourly_from_ts"]:
                print("wwoooooooooooooohoooooooooooo bitch")
                break
            print("sleeping")
            time.sleep(10)
    db.master.update_one(
        {"_id": pair_doc["_id"]},
        {
            "$push": {
                "hourly_crawled_at": datetime.datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            }
        },
    )

    # break
    # time.sleep(15)


def loop_all_exchanges():
    exchanges_list = ["Binance"]
    with open("./cryptocompare/pairs-list.json", "r") as file:
        exchange_json = json.load(file)
    for exchange in exchange_json:
        try:
            if exchange["exchange"] in exchanges_list:
                print(f"Crawling {exchange['pair_sym']} of {exchange['exchange']} ")
                CallAllPairs(exchange["_id"])
        except Exception:
            # If an error occurs during the execution of the target function, do the following:
            # Get error traceback in string format
            traceback_str = traceback.format_exc()

            # Create a dictionary to store information about the error
            error_info = {
                "filename": f"Crypto Compare Historical - {exchange['exchange'] }",  # A placeholder for the filename
                "exchange_id": exchange["_id"],
                "error": traceback_str,  # Record the error message and traceback
                "time": time.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),  # Record the current date and time
            }

            db.Errors.insert_one(error_info)

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

            # Pause execution for 5 seconds before retrying the task
            time.sleep(5)
    mail_data = credentials_data
    mail_data["subject"] = "Badhai Ho!! Apple ka crawler khatam.."
    mail_data[
        "body"
    ] = f"""
            Server Name: Apple Linode
            Server IP:
            Exchanges Completed: {exchanges_list}
            Completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}
            
            Padh liya... ab jaake usko aaghe ka kaam de 😂
        """

    mailResponse = requests.post(mailurl, json=mail_data)


def schedule_task(target_func, interval_minutes, *arg):
    """This function schedules and repeatedly runs a target function at a specified interval.

    Args:
        target_func: The function that you want to run at regular intervals.
        interval_minutes: The time interval, in minutes, at which the function should run.
        *arg: Any additional arguments that the target function may require.
    """
    while True:
        # Call the target function with any provided arguments (*arg)
        target_func(*arg)

        # Pause execution for a specified number of minutes
        time.sleep(interval_minutes * 60)


tCS = threading.Thread(target=schedule_task, args=(loop_all_exchanges, 4320))  # 3 days


# cc_master.add_master_data()
tCS.start()
