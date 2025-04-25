import os
import argparse
import pandas as pd
from logger import Logger
from cassandra.cluster import Cluster


class CassandraDatabaseCreator:
    def __init__(self):
        self.SHOW_LOG = True
        self.logger = Logger(self.SHOW_LOG)
        self.log = self.logger.get_logger(__name__)
        
        self.args = self._parse_arguments()
        
        self.KEYSPACE = os.environ["CASSANDRA_KEYSPACE"]
        self.IP_ADDRESS = os.environ["CASSANDRA_IP_ADDRESS"]
        self.PORT = os.environ["CASSANDRA_PORT"]
        self.timeout = 60
        
        self.cluster = None
        self.session = None

    def _parse_arguments(self):
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
        return parser.parse_args()

    def _get_cassandra_session(self):
        contact_points = self.IP_ADDRESS.split(',')
        self.log.info(f"{contact_points=} {self.PORT=}")
        cluster = Cluster(
            contact_points=contact_points,
            port=self.PORT,
            connect_timeout=self.timeout,
        )
        session = cluster.connect()
        return cluster, session

    def _create_keyspace(self):
        self.log.info(f"Dropping keyspace {self.KEYSPACE} if it exists...")
        self.session.execute(f"DROP KEYSPACE IF EXISTS {self.KEYSPACE}", timeout=self.timeout)

        self.log.info(f"Creating keyspace {self.KEYSPACE} if not exists...")
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS %s
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
            """ % self.KEYSPACE)

        self.session = self.cluster.connect(self.KEYSPACE, wait_for_all_pools=True)

    def _get_column_definitions(self):
        csv_file_path = self.args.csv_path
        columns = pd.read_csv(csv_file_path, delimiter=self.args.delimiter, nrows=1).columns.tolist()
        
        column_definitions = []
        for col in columns:
            if col == 'code' or col == 'IndexColumn':
                col_type = 'text PRIMARY KEY'
            else:
                col_type = 'text'
            column_definitions.append(f"{col.replace('-', '_')} {col_type}")
        return column_definitions

    def _create_table(self):
        column_definitions = self._get_column_definitions()
        
        self.log.info(f"Dropping table {self.args.table_name} if it exists...")
        self.session.execute(f"DROP TABLE IF EXISTS {self.args.table_name}", timeout=self.timeout)
            
        self.log.info(f"Creating table {self.args.table_name} if it not exists...")
        definitions_str = '\t' + ',\n\t'.join(column_definitions)
        table_creation_command = f"""
        CREATE TABLE IF NOT EXISTS {self.args.table_name} (
        {definitions_str}
        )
        """
        print(table_creation_command)
        self.session.execute(table_creation_command, timeout=self.timeout)

    def run(self):
        try:
            self.cluster, self.session = self._get_cassandra_session()
            self._create_keyspace()
            self._create_table()
            
        except Exception as e:
            self.log.error(f"Error occurred while creating database: {e}")

        finally:
            if self.cluster is not None:
                self.cluster.shutdown()


if __name__ == "__main__":
    creator = CassandraDatabaseCreator()
    creator.run()