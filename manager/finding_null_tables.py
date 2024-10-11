import json
from cassandra.cluster import Cluster


def check_table_rows(session, keyspace, table_name):
    query = f"SELECT COUNT(*) FROM {keyspace}.{table_name};"
    try:
        result = session.execute(query)
        count = result.one()[0]
        return count == 0
    except Exception as e:
        print(f"Error querying table {table_name}: {e}")
        return False


def main():
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

    # List to store tables with 0 rows
    empty_tables = []

    # Check each table for 0 rows
    print("Checking tables dynamically:")
    for table in tables:
        table_name = table.table_name
        print(f"Checking table: {table_name}...")
        if check_table_rows(session, keyspace, table_name):
            print(f"Table {table_name} has 0 rows.")
            empty_tables.append(table_name)
        else:
            print(f"Table {table_name} has data.")

    # Write tables with 0 rows to a JSON file
    if empty_tables:
        print("Writing tables with 0 rows to 'empty_tables.json'.")
        with open("empty_tables.json", "w") as json_file:
            json.dump({"empty_tables": empty_tables}, json_file, indent=4)
    else:
        print(f"No empty tables found in keyspace '{keyspace}'.")

    # Close the session and connection
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
