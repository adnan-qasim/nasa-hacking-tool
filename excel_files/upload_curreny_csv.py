import fake_useragent , pymongo
import pandas as pd
from datetime import datetime


fake_user_agent = fake_useragent.FakeUserAgent()

# mongo_uri = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_uri = pymongo.MongoClient("mongodb://user:pass@chongodb.catax.me/")
db = mongo_uri.MasterCC



csv_files = [
    "./excel_files/eur_inr.csv",
    "./excel_files/gbp_inr.csv",
    "./excel_files/usd_inr.csv",
    "./excel_files/gbp_usd.csv",
    "./excel_files/eur_usd.csv",
]

new_data = []
for csv_file in csv_files:
    pair_name = csv_file.split("/")[2].split(".")[0]

    df = pd.read_csv(csv_file)

    records = df.to_dict(orient="records")

    for record in records:
        date_str = record.get("Date")
        if date_str:
            date_object = datetime.strptime(date_str, "%Y-%m-%d")
            record.update(
                {
                    "timestamp": int(date_object.timestamp()),
                }
            )
        found_dict = next(
            (d for d in new_data if d.get("timestamp") == record["timestamp"]), None
        )
        if found_dict:
            for p, v in found_dict["pair_ohlc"].items():
                if pair_name == p:
                    found_dict["pair_ohlc"][p] = {
                        "open": record["Open"],
                        "high": record["High"],
                        "low": record["Low"],
                        "close": record["Close"],
                    }
                    break
        else:
            new_dict = {
                "timestamp": record["timestamp"],
                "date": record["Date"],
                "pair_ohlc": {
                    "gbp_usd": {"open": None, "high": None, "low": None, "close": None},
                    "eur_usd": {"open": None, "high": None, "low": None, "close": None},
                    "usd_inr": {"open": None, "high": None, "low": None, "close": None},
                    "eur_inr": {"open": None, "high": None, "low": None, "close": None},
                    "gbp_inr": {"open": None, "high": None, "low": None, "close": None},
                },
            }
            for p, v in new_dict["pair_ohlc"].items():
                if pair_name == p:
                    new_dict["pair_ohlc"][p] = {
                        "open": record["Open"],
                        "high": record["High"],
                        "low": record["Low"],
                        "close": record["Close"],
                    }
                    break
            new_data.append(new_dict)

db["Currencies_Data"].insert_many(new_data)

mongo_uri.close()
