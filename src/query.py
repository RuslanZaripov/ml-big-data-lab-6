import os
import argparse
from dotenv import load_dotenv

from cassandra.cluster import Cluster


parser = argparse.ArgumentParser(description="Create Database")
parser.add_argument(
    "--table", 
    required=True, 
    help="Table name")
args = parser.parse_args()

load_dotenv()

KEYSPACE = os.getenv("CASSANDRA_KEYSPACE")
IP_ADDRESS = os.getenv("CASSANDRA_IP_ADDRESS")
PORT = os.getenv("CASSANDRA_PORT")
timeout = 60

contact_points = IP_ADDRESS.split(',')

print(f"{contact_points=} {PORT=}")

cluster = Cluster(contact_points, port=PORT, connect_timeout=timeout)
session = cluster.connect(KEYSPACE, wait_for_all_pools=True)

for i, row in session.execute(f"SELECT * FROM {args.table} limit 1", timeout=timeout):
    print(row)
