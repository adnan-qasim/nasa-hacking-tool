import requests, json, traceback
import datetime, fake_useragent 
import pymongo, time, threading, os


mailurl = "https://emailsender.catax.me/sendEmail"



credemtials_data = {
    "username": "AKIAVG3KVGIQ5K5C54EV",
    "password": "BGI30r7ViaHz5pMhtMjkqw/GDeAD4S3McLoMJltIaaqF",
    "server_addr": "email-smtp.eu-north-1.amazonaws.com",
    "server_port": "587",
    "destination_email": "gewgawrav@gmail.com",
    "sender_email": "error@catax.me",
    "subject": "Test Email",
    "body": "This is a test email. Hello from Error!"
}


# Connecting to MongoDB and initializing the database
mongo_uri = pymongo.MongoClient("mongodb://user:pass@mongodb.catax.me/")
dbpx = mongo_uri.ProxiesDatabase
db = mongo_uri.CoinDCXdb


# Fake user agent to send requests anonymously
fake_user_agent = fake_useragent.FakeUserAgent()


def exchange_ticker():
    """
    This function fetches exchange ticker data from a cryptocurrency exchange (Coindcx)
    and stores it in a database. It uses a fake user agent to access the website.
    """
    # Define a user agent to make the request look like it's coming from a web browser
    headers = {"User-Agent": fake_user_agent.chrome}

    # Define the URL of the API endpoint to fetch ticker data
    url = "https://api.coindcx.com/exchange/ticker"

    # Send a GET request to the API with the defined user agent, and convert the response to JSON
    response = requests.get(url, headers=headers).json()

    # For each ticker in the response, do the following:
    for ticker in response:
        # Convert the 'timestamp' value to a human-readable date and time format
        ticker_timestamp = datetime.datetime.fromtimestamp(
            ticker.get("timestamp", 0)
        ).strftime("%Y-%m-%d %H:%M:%S")
        # Update the 'timestamp' in the ticker data with the human-readable format
        ticker["timestamp"] = ticker_timestamp

    # Insert the modified ticker data into a database table called 'ExchangeTickers'
    db.ExchangeTickers.insert_many(response)




def market_details():
    """
    This function fetches market details from a cryptocurrency exchange (Coindcx)
    and stores them in a database. It also saves a list of trading pairs to a local JSON file.
    """
    # Define a user agent to make the request look like it's coming from a web browser
    headers = {"User-Agent": fake_user_agent.chrome}

    # Define the URL of the API endpoint to fetch market details
    url = "https://api.coindcx.com/exchange/v1/markets_details"

    # Send a GET request to the API with the defined user agent, and convert the response to JSON
    response = requests.get(url, headers=headers).json()

    # Create a list of trading pairs that use the INR as the base currency
    pair_list = [
        market["pair"]
        for market in response
        if market["base_currency_short_name"] == "INR"
    ]

    # Insert the market details into a database table called 'MarketDetails'
    db.MarketDetails.insert_many(response)

    # Save the list of trading pairs to a local JSON file called 'coindcx-pair_list.json'
    with open("./coindcx/coindcx-pair_list.json", "w") as f:
        json.dump(pair_list, f, indent=4)




def pair_trades():
    """
    This function fetches recent pair trades for different cryptocurrency pairs
    and stores them in a database.
    """
    # Open a file called 'coindcx-pair_list.json' and read its contents.
    with open("./coindcx/coindcx-pair_list.json", "r") as f:
        # Load the data from the file and store it in a variable called 'pairs'.
        pairs = json.load(f)

    # For each cryptocurrency pair in the 'pairs' list, do the following:
    for pair in pairs:
        # Create a special header for our HTTP request that mimics a web browser
        # (it's like wearing a disguise on the internet).
        headers = {"User-Agent": fake_user_agent.chrome}

        # Build a URL to fetch trade history data for the current cryptocurrency pair.
        url = f"https://public.coindcx.com/market_data/trade_history/?pair={pair}&limit=500"

        # Send an HTTP GET request to the URL using the headers we created,
        # and get the response data in JSON format.
        response = requests.get(url, headers=headers).json()

        # For each trade in the response data, do the following:
        for trade in response:
            # Convert the timestamp of the trade into a human-readable format.
            timestr = datetime.datetime.fromtimestamp(trade["T"] / 1000).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )[:-3]

            # Create a new dictionary called 'new_trade' to store trade information.
            new_trade = {
                "trade_price": trade["p"],
                "quantity": trade["q"],
                "market_symbol": trade["s"],
                "timestamp": timestr,
                "is_buyer_maker": trade["m"],
            }

            # Check if a trade with the same market symbol and timestamp already exists in the database.
            # If not, insert the new trade into the 'RecentTrades' database table.
            if (
                db.RecentTrades.find_one(
                    {"market_symbol": trade["s"], "timestamp": timestr}
                )
                is None
            ):
                db.RecentTrades.insert_one(new_trade)



        # Pause the program for 100 seconds before fetching data for the next pair.
        time.sleep(100)


def candlesticks():
    """
    This function fetches candlestick data for different cryptocurrency pairs
    and stores it in a database. It also handles any potential errors during
    the process.
    """
    # Open a file containing a list of cryptocurrency pairs
    with open("./coindcx/coindcx-pair_list.json", "r") as f:
        pairs = json.load(f)

    # For each cryptocurrency pair in the list, do the following:
    for pair in pairs:
        # Create a custom User-Agent header to make the request look like it's coming
        # from a web browser
        headers = {"User-Agent": fake_user_agent.chrome}

        # Build the URL to fetch candlestick data for the specific pair and time interval
        url = f"https://public.coindcx.com/market_data/candles/?pair={pair}&interval=5m&limit=999"

        # Send a request to the URL using the custom User-Agent header and get the JSON response
        response = requests.get(url, headers=headers).json()

        # For each candlestick in the response, do the following:
        for candle in response:
            # Extract the timestamp from the candlestick data and format it as a string
            candle_time = datetime.datetime.fromtimestamp(
                candle.get("time", 0) / 1000
            ).strftime("%Y-%m-%d %H:%M:%S")

            # Add the formatted timestamp and the cryptocurrency pair to the candlestick data
            candle.update({"time": candle_time, "pair": pair})

            # Check if the same candlestick data is already in the database
            if db.Candlesticks.find_one({"pair": pair, "time": candle_time}) is None:
                # If not, insert the candlestick data into the database
                db.Candlesticks.insert_one(candle)



        # Pause execution for 150 seconds (2.5 minutes) before processing the next pair
        time.sleep(150)


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
                "filename": "CoinDCX",  # A placeholder for the filename
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
            
            mongo_uri.ErrosLogs.Errors.insert_one(error_info)

            #Code to send Email about error
            ErrorData =  credemtials_data
            ErrorData['subject'] =  "Error occured in Coin DCX's Crawler"

            # Replace placeholders with actual values
            ErrorData['body'] = f"""
                Dear Admin,

                We encountered an error in the {error_info["filename"]} data crawler system. Please find the details below:

                - Filename: {error_info["filename"]}
                - Error Time: {error_info["time"]}

                Error Details:

                {error_info["error"]}
                
                We appreciate your prompt attention to this matter. If you need any further information, please feel free to reach out.

                Padh liya?... Ab Jaldi jaake dekh
            """


            mailResponse =  requests.post(mailurl,json=ErrorData)
            # print("Response:")
            # print(mailResponse.status_code)
            # print(mailResponse.text)


            # Pause execution for 5 seconds before retrying the task
            time.sleep(5)


# exchange_ticker()
# market_details()
# pair_trades()
# candlesticks()


# The first thread, 'tEt', is set to run the 'exchange_ticker' function every 60 minutes.
tEt = threading.Thread(target=schedule_task, args=(exchange_ticker, 60))

# The second thread, 'tPT', is set to run the 'pair_trades' function every 720 minutes (12 hours).
tPT = threading.Thread(target=schedule_task, args=(pair_trades, 720))

# The third thread, 'tCS', is set to run the 'candlesticks' function every 4320 minutes (3 days).
tCS = threading.Thread(target=schedule_task, args=(candlesticks, 4320))

# Call the 'market_details' function to gather initial market data.
market_details()

# Start each of the three threads, which will run their respective functions periodically.
tEt.start()  # 'exchange_ticker' function every 60 minutes
# tPT.start()  # 'pair_trades' function every 720 minutes
# tCS.start()  # 'candlesticks' function every 4320 minutes
