from bs4 import BeautifulSoup
import requests, pprint, threading, scheduler
import json, datetime, pytz
import time, fake_useragent, pymongo

# Establish a connection to a MongoDB database using a URI
mongo_uri = pymongo.MongoClient("mongodb://user:pass@mongodb.catax.me/")
db = mongo_uri.ProxiesDatabase

# Create a fake user agent generator
fake_user_agent = fake_useragent.UserAgent()


# Define a function to convert relative time strings to absolute timestamps
def relative_time_converter(time_str):
    """
    This function converts a relative time string (e.g., '5 mins ago') to an absolute timestamp.

    Args:
        time_str (str): A string representing a relative time.

    Returns:
        str: An absolute timestamp in the 'YYYY-MM-DD HH:MM:SS' format.
              Returns None if the input is not a valid relative time string.
    """
    # Define a dictionary to map time units to seconds
    time_units = {
        "sec": 1,
        "secs": 1,
        "min": 60,
        "minutes": 60,
        "mins": 60,
        "hour": 3600,
        "hours": 3600,
    }
    try:
        # Split the input string into parts
        parts = time_str.split()

        if "ago" in parts:
            # If "ago" is present, it's a time in the past
            value = int(parts[0])  # Extract the numeric value
            unit = parts[1]  # Extract the time unit

            # Check if there's an additional unit (e.g., "hour" or "min")
            if len(parts) > 3:
                additional_unit = parts[2]
                additional_value = int(parts[3])
                total_seconds = value * time_units.get(
                    unit
                ) + additional_value * time_units.get(additional_unit)
            else:
                total_seconds = value * time_units.get(unit)

        else:
            # If "ago" is not present, it's from another website
            value = int(parts[0])  # Extract the numeric value
            unit = parts[1]  # Extract the time unit

            # Calculate the total seconds based on the unit
            total_seconds = value * time_units.get(unit)

        # Calculate the new timestamp by subtracting the total seconds from the current time
        new_time = datetime.datetime.now(
            tz=pytz.timezone("Asia/Kolkata")
        ) - datetime.timedelta(seconds=total_seconds)

        # Format the new timestamp as 'YYYY-MM-DD HH:MM:SS' and return it
        return new_time.strftime("%Y-%m-%d %H:%M:%S")
    except:
        # Return None if there's an error in parsing the input
        return None


def test_proxy_status(proxy: dict):
    proxies = {
        "http": "http://" + proxy["Proxy_IP"],
        "https": "https://" + proxy["Proxy_IP"],
    }
    headers = {"User-Agent": fake_user_agent.chrome}
    print(f"Proxy currently being used: {proxies['http']}")
    try:
        response = requests.get(
            "http://request.catax.me/ddwejcl",
            headers=headers,
            proxies=proxies,
            timeout=3,
        )
        proxy.update(
            {
                "Status_Code": response.status_code,
                "Response_Time": response.elapsed.total_seconds(),
                "Usage_Count": 0,
                "Last_Used": None,
            }
        )
        if response.status_code == 200:
            db.ProxyData.insert_one(proxy)
            try:
                with open("proxies-list.json", "r") as file:
                    existing_proxies = json.load(file)
            except FileNotFoundError:
                existing_proxies = []
            if proxy["Proxy_IP"] not in existing_proxies:
                # Adding only the proxy_ip to list of proxies if its working
                existing_proxies.append(proxy["Proxy_IP"])
                print(" Valid Proxy")
                with open("proxies-list.json", "w") as file:
                    json.dump(existing_proxies, file, indent=4)
        return response.status_code
    except Exception as e:
        print(" Useless Proxy")
    return None


def get_proxy_world(n=2):
    # Loop through pages 1 to 5
    for page in range(1, n):
        # Send a GET request to the website with the current page value
        url = f"https://www.freeproxy.world/?type=&anonymity=&country=&speed=&port=&page={page}"
        response = requests.get(url, headers={"User-Agent": fake_user_agent.chrome})
        html = response.text
        all_proxy_data = []

        # Parse the HTML using BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        # Find the table containing proxy information
        table = soup.find("table", class_="layui-table")
        tbody = table.find("tbody")
        rows = tbody.find_all("tr")

        # Loop through the rows and extract the desired information
        for row in rows:
            columns = row.find_all("td")
            if len(columns) >= 8:  # Check if there are enough columns in the row
                ip_address = columns[0].text.strip()
                port = columns[1].text.strip()
                country = columns[2].find("span", class_="table-country").text.strip()
                city = columns[3].text.strip()
                speed = columns[4].find("a").text.strip()
                connection_type = columns[5].text.strip()
                anonymity = columns[6].text.strip()
                last_check = relative_time_converter(columns[7].text.strip())

                # Create a dictionary for each proxy entry
                proxy_entry = {
                    "Source": "proxyworld",
                    "IP_Address": ip_address,
                    "Port": port,
                    "Country": country,
                    "City": city,
                    "Speed": speed,
                    "Type": connection_type,
                    "Anonymity": anonymity,
                    "Last_Check": last_check,
                    "Proxy_IP": f"{ip_address}:{port}",
                    "Collected_At": time.strftime("%Y-%m-%d %H:%M:%S"),
                }
                if (
                    db.ProxyData.find_one({"IP_Address": proxy_entry["IP_Address"]})
                    is None
                ):
                    print(
                        f"\n\n{proxy_entry['Source']} : New IP Proxy Found, testing for proxy..."
                    )

                    responseData = test_proxy_status(proxy_entry)
                    if responseData == 200:
                        print("Data Saved into MongoDb")
                else:
                    print("\n\nIP proxy is already present, skipping...")


def get_proxy_catax():
    response = requests.get(
        "https://proxy.catax.me/get_all/",
        headers={"User-Agent": fake_user_agent.chrome},
    )
    proxiesData = response.json()
    for proxy in proxiesData:
        proxy_entry = {
            "Source": "proxycatax",
            "IP_Address": proxy["proxy"].split(":")[0],
            "Port": proxy["proxy"].split(":")[1],
            "Country": proxy["region"],
            "City": None,
            "Speed": None,
            "Type": proxy["type"],
            "Anonymity": None,
            "Last_Check": proxy["last_time"],
            "Proxy_IP": proxy["proxy"],
            "Collected_At": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        if db.ProxyData.find_one({"IP_Address": proxy_entry["IP_Address"]}) is None:
            print(
                f"\n\n{proxy_entry['Source']} : New IP Proxy Found, testing for proxy..."
            )

            responseData = test_proxy_status(proxy_entry)
            if responseData == 200:
                print("Data Saved into MongoDb")
        else:
            print("\n\nIP proxy is already present, skipping...")


def get_proxy_list():
    #  Send an HTTP GET request to the website
    url = "https://free-proxy-list.net/"
    response = requests.get(url, headers={"User-Agent": fake_user_agent.chrome})

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content of the page using BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")

        # Find the table containing the IP addresses
        ip_table = soup.find("table", {"class": "table-striped"})

        # Initialize a list to store the extracted IP addresses and ports
        all_proxy_list = []

        # Iterate through the rows of the table
        for row in ip_table.find_all("tr"):
            # Find the columns in each row
            columns = row.find_all("td")
            if len(columns) >= 2:
                # Extract the IP address (it's usually in the first column) and the port (second column)
                ip = columns[0].text.strip()
                port = columns[1].text.strip()
                country = columns[3].text.strip()
                Anonymity = columns[4].text.strip()
                HttpType = "https" if columns[6].text.strip() == "yes" else "http"
                last_checked = relative_time_converter(columns[7].text.strip())
                ip_with_port = f"{ip}:{port}"

                proxy_entry = {
                    "Source": "proxylist",
                    "IP_Address": ip,
                    "Port": port,
                    "Country": country,
                    "City": None,
                    "Speed": None,
                    "Type": HttpType,
                    "Anonymity": Anonymity,
                    "Last_Check": last_checked,
                    "Proxy_IP": ip_with_port,
                    "Collected_At": time.strftime("%Y-%m-%d %H:%M:%S"),
                }

                if (
                    db.ProxyData.find_one({"IP_Address": proxy_entry["IP_Address"]})
                    is None
                ):
                    print(
                        f"\n\n{proxy_entry['Source']} : New IP Proxy Found, testing for proxy..."
                    )

                    responseData = test_proxy_status(proxy_entry)
                    if responseData == 200:
                        print("Data Saved into MongoDb")
                else:
                    print("\n\nIP proxy is already present, skipping...")
    else:
        print(
            f"\n\nFailed to retrieve the web page. Status code: {response.status_code}"
        )


def get_proxy_pool():
    url = "https://proxypool.scrape.center/random"
    response = requests.get(url, headers={"User-Agent": fake_user_agent.chrome})
    responseStatusCode = response.status_code
    ip_proxy = response.text.strip()
    if responseStatusCode == 200:
        # for ip_proxy in response.split("\n"):
        proxy_entry = {
            "Source": "proxypool",
            "IP_Address": ip_proxy.split(":")[0],
            "Port": ip_proxy.split(":")[1],
            "Country": None,
            "City": None,
            "Speed": None,
            "Type": None,
            "Anonymity": None,
            "Last_Check": None,
            "Proxy_IP": ip_proxy,
            "Collected_At": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

        if db.ProxyData.find_one({"IP_Address": proxy_entry["IP_Address"]}) is None:
            print(
                f"\n\n{proxy_entry['Source']} : New IP Proxy Found, testing for proxy..."
            )

            responseData = test_proxy_status(proxy_entry)
            if responseData == 200:
                print("Data Saved into MongoDb")
        else:
            print("\n\nIP proxy is already present, skipping...")
    else:
        print(f"\n\nFailed to retrieve the web page. Status code: {responseStatusCode}")


def schedule_task(target_func, interval_minutes, *arg):
    while True:
        target_func()
        time.sleep(interval_minutes * 60)


# calling required methods
tl = threading.Thread(target=schedule_task, args=(get_proxy_list, 12))
tc = threading.Thread(target=schedule_task, args=(get_proxy_catax, 15))
tp = threading.Thread(target=schedule_task, args=(get_proxy_pool, 0.06))
tw = threading.Thread(target=schedule_task, args=(get_proxy_world, 10))


tl.start()
tc.start()
tp.start()
tw.start()
