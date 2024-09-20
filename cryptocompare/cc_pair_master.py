import json
import traceback
import pymongo
from pymongo import InsertOne


mongo_uri = pymongo.MongoClient(
    "mongodb+srv://parth01:parth123@cluster0.77are8z.mongodb.net/?retryWrites=true&w=majority"
)
dbm = mongo_uri.MasterCC
db = mongo_uri.PairsClusterMinutely


def add_master_data():
    try:
        # Load the pair data
        with open("./cryptocompare/pairs_in_how_many_exchanges.json") as f:
            pair_list = json.load(f)

        # Retrieve all pairs in a single query
        pair_symbols = [pair["pair_sym"] for pair in pair_list]
        exchange_data = list(dbm.master.find({"pair_sym": {"$in": pair_symbols}}))

        # Map pair_sym to the list of exchanges
        pair_to_exchanges = {}
        for exch in exchange_data:
            pair_to_exchanges.setdefault(exch["pair_sym"], []).append(exch["exchange"])

        operations = []
        for pairs in pair_list:
            exchanges = pair_to_exchanges.get(pairs["pair_sym"], [])
            pairs.update(
                {
                    "exchanges": exchanges,
                    "hourly_to_timestamp": "",
                    "hourly_from_timestamp": "",
                    "hourly_crawled_at": [],
                    "hourly_entry_count": 0,
                    "minutely_to_timestamp": "",
                    "minutely_from_timestamp": "",
                    "minutely_crawled_at": [],
                    "minutely_entry_count": 0,
                    "daily_to_timestamp": "",
                    "daily_from_timestamp": "",
                    "daily_crawled_at": [],
                    "daily_entry_count": 0,
                }
            )
            # Prepare bulk insert operation
            operations.append(InsertOne(pairs))

        if operations:
            # Perform bulk insert to minimize database round-trips
            result = db.master.bulk_write(operations, ordered=False)
            print(
                f"Inserted {result.inserted_count} documents into 'master' collection."
            )

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    add_master_data()
