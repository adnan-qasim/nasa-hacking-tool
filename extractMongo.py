import pymongo
import json
import os

# MongoDB connection information
mongodb_uri = "mongodb://phantom:sonphantom@ac-bhbxenc-shard-00-00.u8xgxpo.mongodb.net:27017,ac-bhbxenc-shard-00-01.u8xgxpo.mongodb.net:27017,ac-bhbxenc-shard-00-02.u8xgxpo.mongodb.net:27017/?ssl=true&replicaSet=atlas-gkl4j2-shard-0&authSource=admin&retryWrites=true&w=majority"
databases  = ["CoinDCXdb","UnoCoinDatabase","WazirXdb"] 

# Connect to MongoDB
for database_name in databases:
    client = pymongo.MongoClient(mongodb_uri)
    db = client[database_name]

    # Get a list of collection names in the database
    collection_names = db.list_collection_names()

    # Create a directory to store JSON files
    output_directory = "C:\\CryptoCoins_MongoData\\" + database_name
    os.makedirs(output_directory, exist_ok=True)

    # Loop through each collection and retrieve the data
    for collection_name in collection_names:
        collection = db[collection_name]
        cursor = collection.find({})
        
        documents = [document for document in cursor]
        for doc in documents:
            doc["_id"] =  str (doc["_id"])
        
        output_file = os.path.join(output_directory, f"{collection_name}.json")
        with open(output_file, "w") as json_file:
            json.dump(documents, json_file, indent=4)
        
        collection.delete_many({})

    # Disconnect from MongoDB
    client.close()

    print(f"Data from all collections saved in the '{output_directory}' directory.")
