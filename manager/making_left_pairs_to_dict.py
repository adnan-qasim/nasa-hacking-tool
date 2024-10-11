import json

def main():
    # Load the left_pairs_list.json (list of pairs that have no data or don't exist)
    with open("left_pairs_list.json", "r") as left_pairs_file:
        left_pairs_list = json.load(left_pairs_file)

    # Load the sorted_pair_exchanges.json (dictionary with pair as keys)
    with open("sorted_pair_exchanges.json", "r") as sorted_pairs_file:
        sorted_pair_exchanges = json.load(sorted_pairs_file)

    # Create a new dictionary that will hold the matched pairs and their values
    left_pair_dict = {}

    # Iterate through the left_pairs_list and match with sorted_pair_exchanges
    print("Matching pairs from left_pairs_list.json with sorted_pair_exchanges.json...")
    for pair in left_pairs_list:
        if pair in sorted_pair_exchanges:
            left_pair_dict[pair] = sorted_pair_exchanges[pair]  # Add pair and its value to the new dict
            print(f"Matched pair: {pair}")
        else:
            print(f"Pair {pair} not found in sorted_pair_exchanges.json")

    # Write the resulting dictionary to left_pair_dict.json
    if left_pair_dict:
        print("Writing matched pairs to 'left_pair_dict.json'.")
        with open("left_pair_dict.json", "w") as json_file:
            json.dump(left_pair_dict, json_file, indent=4)
    else:
        print("No matching pairs found. 'left_pair_dict.json' not created.")

if __name__ == "__main__":
    main()
