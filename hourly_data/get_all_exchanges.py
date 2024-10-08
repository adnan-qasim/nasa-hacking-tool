import requests
import json

# Define the API endpoint
api_url = "https://min-api.cryptocompare.com/data/v4/all/exchanges"

# Make the API call
response = requests.get(api_url)

# Check if the response was successful
if response.status_code == 200:
    # Parse the response as JSON
    data = response.json()

    # Dictionary to store pairs and the list of exchanges in which they exist
    pair_exchanges = {}

    # Extract exchanges from the data
    exchanges = data.get("Data", {}).get("exchanges", {})

    # Loop through each exchange
    for exchange_name, exchange_info in exchanges.items():
        # Check if the exchange has pairs
        pairs = exchange_info.get("pairs", {})
        # Loop through each coin in the pairs
        for coin, pair_info in pairs.items():
            # Loop through the base currencies (tsyms) for the coin
            for base, details in pair_info.get("tsyms", {}).items():
                # Create the pair string, e.g., "btc_usdt"
                pair = f"{coin.lower()}_{base.lower()}"
                # If the pair is not in the dictionary, add it
                if pair not in pair_exchanges:
                    pair_exchanges[pair] = []
                # Append the exchange name to the list of exchanges for the pair
                pair_exchanges[pair].append(exchange_name)

    # Sorting the pairs based on the length of the exchange list
    sorted_pair_exchanges = dict(sorted(pair_exchanges.items(), key=lambda item: len(item[1]), reverse=True))

    # Write the sorted dictionary to a JSON file
    with open("sorted_pair_exchanges.json", "w") as json_file:
        json.dump(sorted_pair_exchanges, json_file, indent=4)

    output_message = "Pairs and exchanges have been sorted by the number of exchanges and saved to sorted_pair_exchanges.json."
else:
    output_message = f"API call failed with status code: {response.status_code}"

output_message
