import requests, json, traceback
import datetime, pytz, fake_useragent
import pymongo, time, threading, os, sys

parent_directory = os.path.abspath("..")
sys.path.append(parent_directory)
from crypto_crawler.env import *


# Connecting to MongoDB and initializing the database
mongo_uri = pymongo.MongoClient(f"mongodb://{mongo_user_pass}@mongodb.catax.me/")
dbuc = mongo_uri.UnoCoinDatabase

# Fake user agent to send requests anonymously
fake_user_agent = fake_useragent.FakeUserAgent()


def RotateRequest(method, url, data={}, headers={}):
    """
    This function as of now is not rotating from different proxies
    because all the free available proxies are banned or expired.
    We are currently using own IP address with random chrome user agent,
    for sending Request to given url and returning the response in JSON format
    """
    proxy_list = ["121.40.109.225:80", "120.55.75.102:80"]
    response = None
    while len(proxy_list) > 0:
        headers.update({"User-Agent": fake_user_agent.chrome})
        response = requests.request(
            method,
            url,
            headers=headers,
            data=data,
            timeout=None,
        )
        break
    if response == None or response.status_code != 200:
        return None
    return response.json()


def exchange_ticker():
    """This function makes an API request to fetch cryptocurrency ticker data
    from Unocoin. It does this every minute, using different IP addresses and
    user agents, and appends the retrieved data to the same file.
    """
    # Define the URL of the API endpoint
    url = "http://api.unocoin.com/api/v1/exchange/tickers"

    # Create an empty dictionary called 'payload' (we'll use it later)
    payload = {}

    # Send a request to the API endpoint using a function called 'RotateRequest'
    # and specify that we want to use the HTTP GET method to retrieve data
    response = RotateRequest("GET", url, data=payload)

    # Check if we received a valid response from the API
    if response is not None:
        # For each item (we'll call them 'keys') in the response, do the following:
        for key in response:
            # Add a new piece of information called 'timestamp' to each 'key'.
            # The 'timestamp' is the current date and time in a specific format.
            key.update({"timestamp": time.strftime("%Y-%m-%d %H:%M:%S")})

        # Insert the modified response data into a database table called 'UnocoinExcTickers'
        dbuc.UnocoinExcTickers.insert_many(response)


def last_trade():
    """This function makes an API request to fetch historical trade data for
    cryptocurrency pairs from Unocoin. It does this every minute, using different
    IP addresses and user agents, and appends the retrieved data to a database
    if it's not a duplicate.
    """

    # Open a file called 'unocoin-pair-list.json' and load its content as a list
    with open("./unocoin/unocoin-pair-list.json") as F:
        pair_list = json.load(F)

    # Create an empty list called 'timeList' to store timestamps
    timeList = []

    # For each cryptocurrency pair in the 'pair_list', do the following:
    for ticker in pair_list:
        # Get the current time in the 'Asia/Kolkata' timezone
        start = datetime.datetime.now(tz=pytz.timezone("Asia/Kolkata"))

        # Create a URL for the API request using information from 'ticker'
        url = f'http://api.unocoin.com/api/v1/exchange/historical_trades?ticker_id={ticker["ticker_id"]}&depth=1000&type=buy'

        # Create an empty dictionary called 'payload' (we'll use it later)
        payload = {}

        # Send a request to the API endpoint using a function called 'RotateRequest'
        # and specify that we want to use the HTTP GET method to retrieve data
        response = RotateRequest("GET", url, data=payload)

        # Check if we received a valid response from the API
        if response is not None:
            # For each trade data (we'll call them 'keys') in the response, do the following:
            for key in response:
                # Check if the trade data is already in the database 'LastTrades'
                if dbuc.LastTrades.find_one({"trade_id": key["trade_id"]}) is not None:
                    continue  # Skip if it's a duplicate

                # Add additional information to the trade data, such as 'ticker_id'
                # and 'trade_timestamp' in a specific format
                key.update(
                    {
                        "ticker_id": ticker["ticker_id"],
                        "trade_timestamp": datetime.datetime.fromtimestamp(
                            key["trade_timestamp"]
                        ).strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )

                # Insert the modified trade data into the 'LastTrades' database table
                dbuc.LastTrades.insert_one(key)

            # Get the current time again after processing the data
            end = datetime.datetime.now(tz=pytz.timezone("Asia/Kolkata"))

            # Add a record to 'timeList' with details about the processing time
            timeList.append(
                {
                    "ticker": ticker["ticker_id"],
                    "start": start.strftime("%Y-%m-%d %H:%M:%S"),
                    "end": end.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

        # Save the 'timeList' to a file called 'unocoin-time.json'
        with open("./unocoin/unocoin-time.json", "w") as f:
            json.dump(
                timeList,
                f,
                indent=4,
            )


#### Fetch and save cryptocurrency pair list from Unocoin API ####
url = "https://api.unocoin.com/api/v1/exchange/pairs"
payload = {}  # An empty dictionary for additional data (not used here)
files = []  # An empty list for files (not used here)

# Send a GET request to the specified URL with a User-Agent header to simulate a web browser
response = requests.request(
    "GET", url, headers={"User-Agent": fake_user_agent.chrome}
).json()

# For each item (we'll call them 'keys') in the response, do the following:
for key in response:
    # Add a new piece of information called 'timestamp' to each 'key'.
    # The 'timestamp' is the current date and time in a specific timezone (Asia/Kolkata).
    key.update(
        {"timestamp": str(datetime.datetime.now(tz=pytz.timezone("Asia/Kolkata")))}
    )

# Open a file called 'unocoin-pair-list.json' for writing and save the response data in JSON format
with open("./unocoin/unocoin-pair-list.json", "w") as file:
    json.dump(response, file, indent=4)


#### Fetch and save a list of coin types from Unocoin API ####
url = "https://api.unocoin.com/api/coin-type"
payload = {}  # An empty dictionary for additional data (not used here)
files = []  # An empty list for files (not used here)

# Send a POST request to the specified URL with a User-Agent header to simulate a web browser
response = requests.request(
    "POST", url, headers={"User-Agent": fake_user_agent.chrome}, data=payload
).json()

# For each item (we'll call them 'keys') in the response, do the following:
for key in response:
    # Add a new piece of information called 'timestamp' to each 'key'.
    # The 'timestamp' is the current date and time in a specific timezone (Asia/Kolkata).
    key.update(
        {"timestamp": str(datetime.datetime.now(tz=pytz.timezone("Asia/Kolkata")))}
    )

# Open a file called 'unocoin-coin-list.json' for writing and save the response data in JSON format
with open("./unocoin/unocoin-coin-list.json", "w") as file:
    json.dump(response, file, indent=4)


def schedule_task(target_func, interval_minutes, *arg):
    """This function schedules and repeatedly runs a target function at a specified interval.

    Args:
        target_func: The function that you want to run at regular intervals.
        interval_minutes: The time interval, in minutes, at which the function should run.
        *arg: Any additional arguments that the target function may require.
    """
    while True:
        try:
            # Call the target function with any provided arguments (*arg)
            target_func(*arg)

            # Pause execution for a specified number of minutes
            time.sleep(interval_minutes * 60)
        except Exception as e:
            # If an error occurs during the execution of the target function, do the following:
            # Get error traceback in string format
            traceback_str = traceback.format_exc()

            # Create a dictionary to store information about the error
            error_info = {
                "filename": "Unocoin",  # A placeholder for the filename
                "error": traceback_str,  # Record the error message and traceback
                "time": time.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),  # Record the current date and time
            }

            # # Check if an error log file already exists
            # if os.path.exists("error_log.json"):
            #     # If it exists, open it and load its contents into a list
            #     with open("error_log.json", "r") as errorfile:
            #         error_list = json.load(errorfile)
            # else:
            #     # If it doesn't exist, create an empty list
            #     error_list = []
            # # Append the new error information to the list of errors
            # error_list.append(error_info)
            # # Write the updated list of errors back to the error log file
            # with open("error_log.json", "w") as error_file:
            #     json.dump(error_list, error_file, indent=4)

            dbuc.Errors.insert_one(error_info)

            # Code to send Email about error
            ErrorData = credentials_data
            ErrorData["subject"] = "Error occured in UnoCoin's Crawler"

            # Replace placeholders with actual values
            ErrorData[
                "body"
            ] = f"""
                Dear Admin,

                We encountered an error in the {error_info["filename"]} data crawler system. Please find the details below:

                - Filename: {error_info["filename"]}
                - Error Time: {error_info["time"]}

                Error Details:

                {error_info["error"]}
                
                We appreciate your prompt attention to this matter. If you need any further information, please feel free to reach out.

                Padh liya?... Ab Jaldi jaake dekh
            """

            if datetime.datetime.now() >= last_mail + datetime.timedelta(minutes=30):
                mailResponse = requests.post(mailurl, json=ErrorData)
                last_mail = datetime.datetime.now()
            # Pause execution for 5 seconds before retrying the task
            time.sleep(5)


# Create two separate threads to schedule and run two different tasks.

# The first thread, 'tET', is set to run the 'exchange_ticker' function every 1 minute.
tET = threading.Thread(
    target=schedule_task, args=(exchange_ticker, 1)
)  # This task will run every minute

# The second thread, 'tLT', is set to run the 'last_trade' function every 4320 minutes (3 days).
tLT = threading.Thread(
    target=schedule_task, args=(last_trade, 4320)
)  # This task will run every 3 days

# Start both threads to execute their respective tasks concurrently.
tET.start()  # 'exchange_ticker' function every 1 minute
tLT.start()  # 'last_trade' function every 4320 minutes (3 days)
