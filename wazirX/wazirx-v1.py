import requests, json, traceback
import datetime, fake_useragent
import pymongo, time, threading, os, sys
from env import *


# Calculate time 30 minutes ago from the current moment
last_mail = datetime.datetime.now() - datetime.timedelta(minutes=30)

# Fake user agent to send requests anonymously
fake_user_agent = fake_useragent.FakeUserAgent()

# Connecting to MongoDB and initializing the database
mongo_uri = pymongo.MongoClient(f"mongodb://{mongo_user_pass}@tongodb.catax.me/")
db = mongo_uri.WazirXdb


def Exchange_ticker():
    """This function fetches cryptocurrency ticker data from the WazirX API and
    is scheduled to run every 12 hours.
    """
    # Define the URL of the WazirX API endpoint
    url = "https://api.wazirx.com/sapi/v1/tickers/24hr"

    # Create a dictionary called 'headers' that contains information about
    # the user agent and an API key for authentication
    headers = {
        "User-Agent": fake_user_agent.chrome,  # User agent to make the request look like it's coming from a web browser
        "X-Api-Key": "D88mLgkGD6rOpS54eNp0Vy1fUhdYVIbqrNdDkii1eIaSRmP5r1vAkvwOJG3L4313",
    }

    # Send a GET request to the WazirX API using the specified URL and headers
    response = requests.get(url, headers=headers)

    # Convert the response data into a JSON format
    resData = response.json()

    # Create an empty list called 'symbolList'
    symbolList = []

    # For each item (we'll call them 'keys') in the response data, do the following:
    for key in resData:
        # Convert the 'at' timestamp to a human-readable date and time format
        key["at"] = datetime.datetime.fromtimestamp(key["at"] / 1000).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        # Create a dictionary containing 'symbol', 'baseAsset', and 'quoteAsset'
        # information, and add it to the 'symbolList'
        symbolList.append(
            {
                "symbol": key["symbol"],
                "baseAsset": key["baseAsset"],
                "quoteAsset": key["quoteAsset"],
            }
        )

    # Insert the response data into a database table called 'ExchangeTickers'
    db.ExchangeTickers.insert_many(resData)

    # Save the 'symbolList' as a JSON file named 'symbol-pair-list.json'
    with open("./wazirX/symbol-pair-list.json", "w") as f:
        json.dump(symbolList, f, indent=4)


def candlesticks():
    """This function is called every 3 days with a 3-minute delay between requests for different symbols.

    It fetches candlestick data for various cryptocurrency pairs from the WazirX API
    and stores the data in a database collection for each symbol.
    """
    # Open a file that contains a list of cryptocurrency symbols
    with open("./wazirX/symbol-pair-list.json", "r") as f:
        symbols = json.load(f)

    # Iterate over each symbol in the list
    for symbol in symbols:
        # Check if the symbol's quote asset is "inr"
        if symbol["quoteAsset"] == "inr":
            # Define the collection in the database for this symbol
            symColl = db[f"{symbol['symbol']}"]

            # Construct the URL to fetch candlestick data for the symbol
            url = f"https://api.wazirx.com/sapi/v1/klines?symbol={symbol['symbol']}&limit=999&interval=5m"

            # Define the headers for the HTTP request, including a user agent and an API key
            headers = {
                "User-Agent": fake_user_agent.chrome,  # Simulated user agent for the request
                "X-Api-Key": "D88mLgkGD6rOpS54eNp0Vy1fUhdYVIbqrNdDkii1eIaSRmP5r1vAkvwOJG3L4313",  # API key
            }

            # Send an HTTP GET request to the WazirX API with the defined URL and headers
            response = requests.get(url, headers=headers)

            # Parse the JSON response from the API
            resData = response.json()

            # Iterate over the retrieved candlestick data
            for data in resData:
                # Create a new data object with relevant information
                NewData = {
                    "symbol": symbol,  # The cryptocurrency symbol
                    "startTime": datetime.datetime.fromtimestamp(data[0]).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),  # Start time
                    "open": data[1],  # Opening price
                    "high": data[2],  # Highest price
                    "low": data[3],  # Lowest price
                    "close": data[4],  # Closing price
                    "volume": data[5],  # Trading volume
                }

                # Check if the data with the same start time already exists in the collection
                if symColl.find_one({"startTime": NewData["startTime"]}) is None:
                    # If not, insert the new data into the collection
                    symColl.insert_one(NewData)

            # Pause execution for 200 seconds (3 minutes and 20 seconds) before fetching data for the next symbol
            time.sleep(200)


def RecentTrades():
    """This function retrieves recent trade data for cryptocurrency pairs with INR as the quote asset.

    It fetches trade data from the WazirX API for different symbols, and for each trade, it stores
    the data in a database collection named 'RecentTrades'.
    """
    # Open a file that contains a list of cryptocurrency symbols
    with open("./wazirX/symbol-pair-list.json", "r") as f:
        symbols = json.load(f)

    # Iterate over each symbol in the list
    for symbol in symbols:
        # Check if the symbol's quote asset is "inr"
        if symbol["quoteAsset"] == "inr":
            # Construct the URL to fetch recent trade data for the symbol
            url = f"https://api.wazirx.com/sapi/v1/trades?symbol={symbol['symbol']}&limit=999"

            # Define the headers for the HTTP request, including a user agent and an API key
            headers = {
                "User-Agent": fake_user_agent.chrome,  # Simulated user agent for the request
                "X-Api-Key": "D88mLgkGD6rOpS54eNp0Vy1fUhdYVIbqrNdDkii1eIaSRmP5r1vAkvwOJG3L4313",  # API key
            }

            # Send an HTTP GET request to the WazirX API with the defined URL and headers
            response = requests.get(url, headers=headers)

            # Parse the JSON response from the API
            resData = response.json()

            # Iterate over the retrieved trade data
            for key in resData:
                # Update the trade data with additional information, including the symbol and time
                key.update(
                    {
                        "symbol": symbol["symbol"],  # The cryptocurrency symbol
                        "time": datetime.datetime.fromtimestamp(
                            key["time"] / 1000
                        ).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),  # Convert timestamp to human-readable time
                    }
                )

                # Check if the trade data with the same 'id' already exists in the collection
                if db.RecentTrades.find_one({"id": key["id"]}) is None:
                    # If not, insert the new trade data into the 'RecentTrades' collection
                    db.RecentTrades.insert_one(key)

            # Pause execution for 150 seconds (2 minutes and 30 seconds) before fetching data for the next symbol
            time.sleep(150)


def schedule_task(target_func, interval_minutes, *arg):
    """This function schedules and repeatedly runs a target function at a specified interval.

    Args:
        target_func: The function that you want to run at regular intervals.
        interval_minutes: The time interval, in minutes, at which the function should run.
        *arg: Any additional arguments that the target function may require.
    """
    global last_mail
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
                "filename": "WazirX",  # A placeholder for the filename
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

            db.Errors.insert_one(error_info)

            # Code to send Email about error
            ErrorData = credentials_data
            ErrorData["subject"] = "Error occured in WazirX's Crawler"

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

            if datetime.datetime.now() >= last_mail + datetime.timedelta(minutes=30):
                mailResponse = requests.post(mailurl, json=ErrorData)
                last_mail = datetime.datetime.now()

            # Pause execution for 5 seconds before retrying the task
            time.sleep(5)


# Create three threads that will run different functions at specified intervals.
# The first thread, 'tET', is set to run the 'Exchange_ticker' function every 180 minutes (3 hours).
tET = threading.Thread(target=schedule_task, args=(Exchange_ticker, 180))

# The second thread, 'tCS', is set to run the 'candlesticks' function every 4320 minutes (3 days).
tCS = threading.Thread(target=schedule_task, args=(candlesticks, 4320))

# The third thread, 'tRT', is set to run the 'RecentTrades' function every 1440 minutes (1 day).
tRT = threading.Thread(target=schedule_task, args=(RecentTrades, 1440))

# Start each of the three threads, which will run their respective functions periodically.
tET.start()  # 'Exchange_ticker' function every 180 minutes (3 hours)
tCS.start()  # 'candlesticks' function every 4320 minutes (3 days)
tRT.start()  # 'RecentTrades' function every 1440 minutes (1 day)
