import os

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

KEYSPACE = os.getenv("CASSANDRA_KEYSPACE")
IP_ADDRESS = os.getenv("CASSANDRA_IP_ADDRESS")
PORT = os.getenv("CASSANDRA_PORT")
timeout = 60

contact_points = IP_ADDRESS.split(',')

print(f"{contact_points=} {PORT=}")

cluster = Cluster(contact_points, port=PORT, connect_timeout=timeout)
session = cluster.connect(KEYSPACE, wait_for_all_pools=True)

query = SimpleStatement("""
    INSERT INTO mytable (thekey, col1, col2)
    VALUES (%(key)s, %(a)s, %(b)s)
    """, 
    consistency_level=ConsistencyLevel.ONE)

prepared = session.prepare("""
    INSERT INTO mytable (thekey, col1, col2)
    VALUES (?, ?, ?)
    """)

for i in range(10):
    print("inserting row %d" % i)
    session.execute(query, dict(key="key%d" % i, a='a', b='b'))
    session.execute(prepared, ("key%d" % i, 'b', 'b'))
