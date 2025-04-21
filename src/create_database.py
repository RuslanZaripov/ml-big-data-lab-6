import os
import argparse
import pandas as pd

from cassandra.cluster import Cluster

parser = argparse.ArgumentParser(description="Create Database")
parser.add_argument(
    "--table-name", 
    required=True, 
    help="Table name")
parser.add_argument(
    "--csv-path", 
    required=True, 
    help="Path to the CSV file")
parser.add_argument(
    "--delimiter", 
    default=None, 
    help="Delimiter used in the CSV file (default: autodetect)")
args = parser.parse_args()

KEYSPACE = os.environ["CASSANDRA_KEYSPACE"]
IP_ADDRESS = os.environ["CASSANDRA_IP_ADDRESS"]
PORT = os.environ["CASSANDRA_PORT"]
timeout = 60

def get_cassandra_session():
    contact_points = IP_ADDRESS.split(',')
    print(f"{contact_points=} {PORT=}")
    cluster = Cluster(
        contact_points=contact_points,
        port=PORT,
        connect_timeout=timeout,
    )
    session = cluster.connect()
    return cluster, session

try:
    cluster, session = get_cassandra_session()

    print("creating keyspace...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """ % KEYSPACE)

    session = cluster.connect(KEYSPACE, wait_for_all_pools=True)

    csv_file_path = args.csv_path
    columns = pd.read_csv(csv_file_path, delimiter=args.delimiter, nrows=1).columns.tolist()
    print(f"{columns=}")

    column_definitions = []
    for col in columns:
        if col == 'code' or col == 'IndexColumn':
            col_type = 'text PRIMARY KEY'
        else:
            col_type = 'text'
        column_definitions.append(f"{col.replace('-', '_')} {col_type}")
        
    print(f"dropping table {args.table_name} if it exists...")
    session.execute(f"DROP TABLE IF EXISTS {args.table_name}", timeout=timeout)
        
    print(f"creating table {args.table_name} if it not exists...")
    definitions_str = '\t' + ',\n\t'.join(column_definitions)
    table_creation_command = f"""
    CREATE TABLE IF NOT EXISTS {args.table_name} (
    {definitions_str}
    )
    """
    print(table_creation_command)
    session.execute(table_creation_command, timeout=timeout)
    
except Exception as e:
    raise

finally:
    if 'cluster' in locals():
        cluster.shutdown()
