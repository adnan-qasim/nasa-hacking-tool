# Import necessary libraries
import requests  # For making HTTP requests to web services
import json  # To work with JSON data
import traceback  # For detailed error tracking
import datetime  # For working with dates and times
import fake_useragent  # To generate fake browser user agents
import pymongo  # MongoDB driver for Python
import time  # For working with time-related functions
import threading  # For running tasks in parallel threads
import cc_minutely_master as cc_minutely_master  # Specific module for crypto data
import signal  # To handle interrupt signals
import sys  # For using system-specific parameters and functions
from pymongo.collection import (
    ReturnDocument,
)  # For returning documents after updates in MongoDB
from apscheduler.schedulers.background import (
    BackgroundScheduler,
)  # For scheduling tasks to run in the background
from apscheduler.triggers.cron import (
    CronTrigger,
)  # For scheduling tasks using cron syntax
from apscheduler.triggers.interval import (
    IntervalTrigger,
)  # For scheduling tasks at regular intervals

# Global variable to store the server's name
server_name = ""

# Calculate time 30 minutes ago from the current moment
last_mail = datetime.datetime.now() - datetime.timedelta(minutes=30)

# Create a fake user agent to mimic a web browser
fake_user_agent = fake_useragent.FakeUserAgent()

# Access the database and server logs database from the imported module
db = cc_minutely_master.db
dbs = cc_minutely_master.mongo_uri.ServerLogsDB


# Function to fetch OHLCV data for a given cryptocurrency pair from an exchange
def GetPairOHLCV(exchange: str, pair: str, limit: int = 1999):
    # Print a message indicating the task
    print(f"getting ohlcv of {pair} in {exchange}")

    # Access the collection for the specified pair
    collection = db[f"{pair}"]

    # Set the user agent to the fake one generated earlier
    headers = {"User-Agent": fake_user_agent.chrome}

    # Split the pair to get the symbols for the cryptocurrencies
    fsym, tsym = pair.split("_")[0], pair.split("_")[1]

    # Construct the URL for the API request
    url = f"https://min-api.cryptocompare.com/data/v2/histominute?fsym={fsym}&tsym={tsym}&limit={limit}&e={exchange}"

    # Make the HTTP request and parse the JSON response
    response = requests.get(url, headers=headers).json()

    # If the response is successful, process the data
    if response["Response"] == "Success":
        new = []
        # Iterate over the OHLCV data points in the response
        for ohlcv in response["Data"]["Data"]:
            # Convert the timestamp to a readable format and add the exchange name
            ohlcv.update(
                {
                    "time": datetime.datetime.fromtimestamp(ohlcv["time"]).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "exchange": exchange,
                }
            )
            # Add the updated OHLCV data to the new list
            new.append(ohlcv)

        # Fetch existing data from the collection to prevent duplicates
        old = list(
            collection.find({"exchange": exchange}, projection={"_id": False})
            .sort([("time", pymongo.DESCENDING)])
            .limit(2002)
        )

        # Determine which new data points do not exist in the collection
        writable = []
        for i in range(len(new)):
            if new[i] not in old:
                writable.append(new[i])

        # If there's nothing new to write, return an error message
        if len(writable) == 0:
            return "Error"

        # Insert new data points into the collection
        collection.insert_many(writable)

        # Print a message indicating how many data points were added
        print(f"{len(writable)} ohlcv of {pair} in {exchange} crawled")

        # Return information about the time range and count of data points added
        return [response["Data"]["TimeTo"], response["Data"]["TimeFrom"], len(writable)]

    # Handle rate limiting by the API
    elif (
        response["Message"]
        == "You are over your rate limit please upgrade your account!"
    ):
        # Extract the rate limit information from the response
        calls_made = response["RateLimit"]["calls_made"]
        for t, v in response["RateLimit"]["max_calls"].items():
            # If the limit is exceeded, wait until the limit resets
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
        # Retry fetching the data after handling rate limiting
        return GetPairOHLCV(exchange, fsym, tsym, limit)

    else:
        # If the response is not successful and it's not a rate limit issue, raise an exception
        raise Exception(response)


# Function to fetch data for all exchanges listed for a specific cryptocurrency pair
def GetAllExchanges(pair: str):
    global last_mail
    startTime = datetime.datetime.now()  # Record the start time of data fetching
    # Retrieve the document for the pair from the master collection
    doc = db.master.find_one({"pair_sym": pair})
    total_crawled = doc["minutely_entry_count"]  # Get the current count of entries

    # Iterate over each exchange listed for the pair
    for exchange in doc["exchanges"]:
        try:
            # Attempt to fetch OHLCV data for the pair from the exchange
            result = GetPairOHLCV(exchange, pair)
            if result == "Error":
                continue  # If no new data was fetched, move to the next exchange
            total_crawled += result[2]  # Add the count of new data points fetched

            # Format and calculate the latest and oldest timestamps of fetched data
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

            # Update the document in the master collection with the new timestamps
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
            # If an error occurs, log it, remove the exchange from the list, and notify via email
            db.master.update_one(
                {"pair_sym": pair},
                {"$pull": {"exchanges": exchange}, "$push": {"deprecated": exchange}},
            )
            traceback_str = traceback.format_exc()  # Capture the error details
            # Prepare the error information for logging
            error_info = {
                "filename": f"Crypto Compare Minutely : {pair} -> {exchange}",
                "server": server_name,
                "error": traceback_str,
                "time": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            # Insert the error information into the PairErrors collection
            db.PairErrors.insert_one(error_info)

            # Code segment to send an email notification about the error
            # Prepare the email data
            ErrorData = cc_minutely_master.credentials_data
            ErrorData["subject"] = "Error occurred in CryptoCompare's Crawler"
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
            # Send the email if there is no mail sent in previous 30 minutes
            if datetime.datetime.now() >= last_mail + datetime.timedelta(minutes=30):
                mailResponse = requests.post(cc_minutely_master.mailurl, json=ErrorData)
                # ErrorData["destination_email"] = "shiekh111aq@gmail.com"
                # mailResponse = requests.post(cc_minutely_master.mailurl, json=ErrorData)
                last_mail = datetime.datetime.now()

        finally:
            # Regardless of success or failure, wait for 12 seconds before the next iteration
            time.sleep(12)

    # Once all exchanges have been processed, update the master document with the final counts and timestamps
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
    Processes odd-indexed cryptocurrency pairs from a predefined list, fetching their data through GetAllExchanges function.
    If it's been more than 30 minutes since the last email was sent, this function also sends an update via email.
    """
    global last_mail  # Access the global variable to check/update the last email sent time.

    # Open the JSON file containing the list of cryptocurrency pairs to process.
    with open("./cryptocompare/pairs_list_for_minutely.json") as f:
        pair_list = json.load(f)  # Load the list of pairs from the JSON file.

    # Filter and process odd indexed pairs from the list (actual filtering logic to be implemented as needed).
    pair_list = pair_list[a::]  # @todo change it accordingly
    for pair in pair_list:
        GetAllExchanges(
            pair["pair_sym"]
        )  # Fetch and process data for each selected pair.

    # Prepare email data for notification after completing the task.
    mail_data = (
        cc_minutely_master.credentials_data
    )  # Use the predefined email credentials/configurations.
    mail_data["sender_email"] = "badhai@catax.me"  # Set the sender email address.
    mail_data["subject"] = (
        "Badhai Ho!! aaj ka minutely crawler ka odd index pairs khatam.."  # Set the email subject.
    )
    # Compose the email body with completion details.
    mail_data[
        "body"
    ] = f"""
        Name: Minutely Odd Index
        Server: {server_name}
        Exchanges Completed: {len(pair_list)}
        Completed at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        Padh liya... ab jaake usko aaghe ka kaam de 😂
    """

    # Check if it's been more than 30 minutes since the last email was sent.
    if datetime.datetime.now() >= last_mail + datetime.timedelta(minutes=30):
        mailResponse = requests.post(
            cc_minutely_master.mailurl, json=mail_data
        )  # Send the email.
        last_mail = datetime.datetime.now()  # Update the last mail sent time.


# Similar to odd_pairs, even_pairs would process even-indexed cryptocurrency pairs.
def even_pairs():
    """
    Similar to odd_pairs, this function is intended to process even-indexed cryptocurrency pairs.
    """
    global last_mail
    # Opening the JSON file to read the list of pairs
    with open("./cryptocompare/pairs_list_for_minutely.json") as f:
        pair_list = json.load(f)

    # Iterate over even indexed pairs from the first 23 pairs in the list
    pair_list = pair_list[a::]  # @todo change it accordingly
    for pair in pair_list:
        GetAllExchanges(pair["pair_sym"])

    # Prepare the email data using the cc_minutely_master.credentials_data variable
    mail_data = cc_minutely_master.credentials_data
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

        Padh liya... ab jaake usko aaghe ka kaam de 😂
    """

    # Send the email using a POST request to the cc_minutely_master.mailurl
    if datetime.datetime.now() >= last_mail + datetime.timedelta(minutes=30):
        mailResponse = requests.post(cc_minutely_master.mailurl, json=mail_data)
        last_mail = datetime.datetime.now()


def heartbeat():
    """
    Periodically called to insert a 'heartbeat' into the database, indicating the script is running.
    Cleans up old heartbeats older than 30 days to keep the collection size manageable.
    """
    insert_data = {
        "server": server_name,  # Identify the server where the script is running.
        "message": "alive",  # Indicate the script is alive.
        "time": datetime.datetime.now(),  # Timestamp of the heartbeat.
    }
    # Remove heartbeats older than 30 days for this server.
    dbs.heartbeat.delete_many(
        {
            "server": insert_data["server"],
            "time": {"$lt": datetime.datetime.now() - datetime.timedelta(days=30)},
        }
    )
    # Insert the new heartbeat.
    dbs.heartbeat.insert_one(insert_data)


def schedule_functions():
    """
    Sets up scheduled tasks using APScheduler, such as running odd_pairs, even_pairs, and the heartbeat function.
    """
    scheduler = BackgroundScheduler()  # Create a background scheduler.
    # Schedule odd_pairs to run daily at a specified time (here, 12:00:00 PM).
    scheduler.add_job(
        odd_pairs, trigger=CronTrigger(hour=12, minute=0, second=0, day_of_week="*")
    )
    # Schedule even_pairs to run immediately after odd_pairs.
    scheduler.add_job(
        even_pairs, trigger=CronTrigger(hour=12, minute=0, second=30, day_of_week="*")
    )
    # Schedule heartbeat to run every 15 minutes.
    scheduler.add_job(heartbeat, trigger=IntervalTrigger(minutes=15))
    scheduler.start()  # Start the scheduler.

    # Function to handle termination signals, cleanly shutting down the scheduler.
    def signal_handler(signal, frame):
        print("Received termination signal. Shutting down...")
        scheduler.shutdown()  # Shutdown the scheduler.
        sys.exit(0)  # Exit the script.

    # Listen for SIGINT (e.g., Ctrl+C) to trigger the signal handler.
    signal.signal(signal.SIGINT, signal_handler)


def run_schedule():
    """
    Keeps the script running indefinitely, allowing scheduled tasks to execute.
    """
    while True:
        time.sleep(10)  # Sleep for a short time to prevent the script from exiting.


# Main execution block to start scheduled functions and handle errors.
try:
    # cc_minutely_master.add_master_data()  # Add master data for the minutely crawler.
    schedule_functions()  # Set up and start the scheduled tasks.

    # Start processing odd and even pairs in parallel threads for immediate execution.
    threading.Thread(target=odd_pairs).start()
    time.sleep(
        30
    )  # Wait a bit before starting the even_pairs to stagger their execution.
    threading.Thread(target=even_pairs).start()

    run_schedule()  # Keep the script running for scheduled tasks to execute.
except Exception as e:  # Catch any exceptions that occur during setup or execution.
    traceback_str = traceback.format_exc()  # Capture the detailed traceback.
    # Log the error to the database for troubleshooting.
    insert_data = {
        "server": server_name,
        "message": f"Code had an Error: {traceback_str}",
        "time": datetime.datetime.now(),
    }
    dbs.Errors.insert_one(insert_data)
    sys.exit(1)  # Exit the script with an error status.
