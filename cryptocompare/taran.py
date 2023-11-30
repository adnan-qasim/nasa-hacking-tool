import pymongo
import pandas as pd
import time
import requests, json, traceback
from datetime import datetime

mailurl = "https://emailsender.catax.me/sendEmail"


credentials_data = {
    "username": "AKIAVG3KVGIQ5K5C54EV",
    "password": "BGI30r7ViaHz5pMhtMjkqw/GDeAD4S3McLoMJltIaaqF",
    "server_addr": "email-smtp.eu-north-1.amazonaws.com",
    "server_port": "587",
    "destination_email": "shiekh111aq@gmail.com",
    "sender_email": "cleaner@catax.me",
    "subject": "Test Email",
    "body": "This is a test email. Hello from Error!",
}


# Set up your MongoDB connection
client = pymongo.MongoClient("mongodb://user:pass@chongodb.catax.me")
database_name = "MasterCC"


# Access the specified database
db = client[database_name]

# Define the collection names
collection_names = db.list_collection_names()

# Connect to your local MongoDB
local_db_url = "mongodb://user:pass@mongodb.catax.me"
client_local = pymongo.MongoClient(local_db_url)
local_database = "PAIR_CLUSTER"

# Create a new collection named 'INDEX' to store loop details
index_collection = client_local[local_database]["index"]

# Measure the start time
start_time = time.time()

# Iterate through each collection name
for collection_name in collection_names:
    try:
        # Access the specified collection
        collection = db[collection_name]

        # Retrieve data from the collection
        cursor = collection.find()

        # Convert the cursor to a Pandas DataFrame
        df = pd.DataFrame(list(cursor))

        # Get unique values of parent_sym and child_sym
        unique_parent_sym_values = df["parent_sym"].unique()
        unique_child_sym_values = df["child_sym"].unique()

        # Iterate through unique parent_sym and child_sym combinations
        for parent_sym_value in unique_parent_sym_values:
            for child_sym_value in unique_child_sym_values:
                # Initialize variables to store loop details
                total_documents = 0
                deleted_documents = 0

                # Select rows for the current combination of parent_sym and child_sym
                subset_rows = df[
                    (df["parent_sym"] == parent_sym_value)
                    & (df["child_sym"] == child_sym_value)
                ]

                # Count total number of documents
                total_documents += len(subset_rows)

                # Select rows where all four columns have a value of zero
                condition = (
                    (subset_rows["open"] == 0)
                    & (subset_rows["high"] == 0)
                    & (subset_rows["low"] == 0)
                    & (subset_rows["close"] == 0)
                )

                # Select rows based on the condition
                df_selected = subset_rows[~condition]

                # Count deleted number of documents
                deleted_documents += len(subset_rows) - len(df_selected)

                # Convert the "time" column to datetime for proper sorting
                df_selected["time"] = pd.to_datetime(
                    df_selected["time"], format="%Y-%m-%d %H:%M:%S"
                )

                # Sort the DataFrame based on the "time" column
                df_sorted = df_selected.sort_values(by="time")

                # Calculate the mean of "open" and "close" for each row
                df_sorted["mean_open_close"] = df_sorted[["open", "close"]].mean(axis=1)

                # Drop the specified column
                df_sorted = df_sorted.drop(columns=["conversionType"])
                df_sorted = df_sorted.drop(columns=["conversionSymbol"])

                # Add new columns with the same value for all rows
                df_sorted["source"] = "crypto-compare"
                df_sorted["exchange"] = collection_name
                df_sorted["candle"] = "hourly"

                # Specify the collection name where you want to save the cleaned data
                local_collection_name = f"{parent_sym_value}_{child_sym_value}"

                # Convert the DataFrame to a NumPy array for MongoDB insertion
                # Assuming df_sorted is your DataFrame
                data_to_insert = df_sorted.to_dict(orient="records")

                # Insert data into the local MongoDB collection
                if data_to_insert:
                    client_local[local_database][local_collection_name].insert_many(
                        data_to_insert
                    )

                    # Create a document with loop details
                    loop_details = {
                        "exchange": collection_name,
                        "parent_sym": parent_sym_value,
                        "child_sym": child_sym_value,
                        "start_date": df_sorted["time"].min(),
                        "end_date": df_sorted["time"].max(),
                        "total_documents": total_documents,
                        "deleted_documents": deleted_documents,
                        "type": "hourly",
                        "processed_at": datetime.now(),
                    }
                    # Insert the document into the INDEX collection
                    index_collection.insert_one(loop_details)

                else:
                    print("Warning: 'processed_data' is an empty list.")
    except Exception:
        # If an error occurs during the execution of the target function, do the following:
        # Get error traceback in string format
        traceback_str = traceback.format_exc()

        # Create a dictionary to store information about the error
        error_info = {
            "filename": f"Data Cleaner - {collection_name}",  # A placeholder for the filename
            "pair": local_collection_name or "Unavailable",
            "error": traceback_str,  # Record the error message and traceback
            "time": time.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),  # Record the current date and time
        }

        db.errors.insert_one(error_info)

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


# Measure the end time
end_time = time.time()

# Calculate the execution time
execution_time = end_time - start_time

print(f"Execution time: {execution_time} seconds")

# Close the MongoDB connections
client.close()
client_local.close()


ErrorData = credentials_data
ErrorData["subject"] = "Crypto Compare Cleaner Finished "

# Replace placeholders with actual values
ErrorData[
    "body"
] = f"""
    Just a mail to notify Crypto Compare ka Cleaner Finish ho gya hai. Fucking Delete that costly server. 
    
    Time taken : {execution_time}
        """

mailResponse = requests.post(mailurl, json=ErrorData)
