import json
from cassandra.cluster import Cluster


def check_table_rows(session, keyspace, table_name):
    query = f"SELECT COUNT(*) FROM {keyspace}.{table_name};"
    try:
        result = session.execute(query)
        count = result.one()[0]
        return count == 0  # Return True if the table has 0 rows
    except Exception as e:
        print(f"Error querying table {table_name}: {e}")
        return None  # Return None if there's an error (e.g., table does not exist)


def main():
    # Load all pairs from all_pairs_list.json
    with open("all_pairs_list.json", "r") as json_file:
        all_pairs_list = json.load(json_file)

    cluster = Cluster(
        ["164.52.214.75"],
        connect_timeout=60,
        control_connection_timeout=60,
    )
    session = cluster.connect()

    # Specify the keyspace you want to check
    keyspace = "historical_krishna"

    # Fetch all tables from the keyspace
    query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}';"
    tables = session.execute(query)

    # Convert table names to a set for faster lookup
    table_names = {table.table_name for table in tables}

    # List to store pairs where the corresponding table has 0 rows or does not exist
    left_pairs_list = []

    # Iterate through the all_pairs_list
    print("Checking pairs and matching tables with 0 rows or non-existing tables:")
    for pair in all_pairs_list:
        table_name = f"p_{pair}"  # Add the 'p_' prefix to the pair name
        if table_name in table_names:
            print(f"Checking table: {table_name}...")
            has_zero_rows = check_table_rows(session, keyspace, table_name)
            if has_zero_rows:
                print(f"Table {table_name} has 0 rows. Adding {pair} to left_pairs_list.")
                left_pairs_list.append(pair)  # Add the original pair (without 'p_') to the list
        else:
            print(f"Table {table_name} not found in the keyspace. Adding {pair} to left_pairs_list.")
            left_pairs_list.append(pair)  # Add the original pair to the list if the table does not exist

    # Write the left_pairs_list to a new JSON file
    if left_pairs_list:
        print("Writing left_pairs_list to 'left_pairs_list.json'.")
        with open("left_pairs_list.json", "w") as json_file:
            json.dump(left_pairs_list, json_file, indent=4)
    else:
        print(f"No tables with 0 rows or missing tables were found in keyspace '{keyspace}'.")

    # Close the session and connection
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
