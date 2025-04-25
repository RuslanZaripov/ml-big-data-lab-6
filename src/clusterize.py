import os
import configparser
import argparse
from logger import Logger
from dotenv import load_dotenv
from functools import reduce
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col

SHOW_LOG = True


class Clusterizer():
    def __init__(self):
        logger = Logger(SHOW_LOG)
        self.config = configparser.ConfigParser()
        self.log = logger.get_logger(__name__)
        
        load_dotenv()
        
        spark_config_apth = 'conf/spark.ini'
        
        self.KEYSPACE = os.environ["CASSANDRA_KEYSPACE"]
        IP_ADDRESS = os.environ["CASSANDRA_IP_ADDRESS"]
        PORT = os.environ["CASSANDRA_PORT"]
        
        self.config = configparser.ConfigParser()
        self.config.optionxform = str
        self.config.read(spark_config_apth)
        
        self.model_save_path = self.config['model']['save_path']
        
        conf = SparkConf()
        conf.setAll(list(self.config['spark'].items()))
        
        self.spark = SparkSession.builder.config(conf=conf) \
            .config("spark.cassandra.input.fetch.sizeInRows", 1000) \
            .config("spark.cassandra.input.split.sizeInMB", 32) \
            .config("spark.cassandra.query.retry.count", -1) \
            .config("spark.cassandra.connection.host", IP_ADDRESS) \
            .config("spark.cassandra.connection.port", PORT) \
            .getOrCreate()
                        
        self.best_model = None
        self.best_result = None
        
    def prepare_df(self, df):
        useful_cols = [
            'code',
            'energy_kcal_100g',
            'fat_100g',
            'carbohydrates_100g',
            'sugars_100g',
            'proteins_100g',
            'salt_100g',
            'sodium_100g',
        ]
        
        metadata_cols = ['code']
        feature_cols = [c for c in useful_cols if c not in metadata_cols]
        
        # an energy-amount of more than 1000kcal 
        # (the maximum amount of energy a product can have; 
        # in this case it would conists of 100% fat)
        df = df.filter(col('energy_kcal_100g') < 1000)
        
        # a feature (except for the energy-ones) higher than 100g
        columns_to_filter = [c for c in df.columns if c != 'energy_kcal_100g' and c not in metadata_cols]
        condition = reduce(
            lambda a, b: a & (col(b) < 100),
            columns_to_filter,
            col(columns_to_filter[0]) < 100 
        )
        df = df.filter(condition)
        
        # a feature with a negative entry
        condition = reduce(
            lambda a, b: a & (col(b) >= 0),
            feature_cols,
            col(feature_cols[0]) >= 0 
        )
        df = df.filter(condition)
        
        cluster_df = VectorAssembler(
            inputCols=feature_cols, 
            outputCol="features"
        ).transform(df)
        
        return cluster_df
        
    def cluster(self, k, seed):
        kmeans = KMeans(k=k).setSeed(seed)
        self.kmeans_model = kmeans.fit(self.initial_df)
        return kmeans, self.kmeans_model.transform(self.initial_df)
    
    def evaluate(self, clustered_df):
        evaluator = ClusteringEvaluator(metricName="silhouette", distanceMeasure="squaredEuclidean")
        score = evaluator.evaluate(clustered_df)
        return score
    
    def run(self, table_name):
        df = self.spark.read.format("org.apache.spark.sql.cassandra") \
            .options(table=table_name, keyspace=self.KEYSPACE) \
            .load()
            
        df = self.prepare_df(df)    
        
        model, result = self.cluster(
            self.config['model']['k'], 
            self.config['model']['seed'])
        
        score = self.evaluate(result)
        self.log.info(f"Silhouette Score: {score}")
         
        model.save(self.model_save_path)
        self.log.info(f"Model saved to {self.model_save_path}")
        
        self.spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Pipeline")
    parser.add_argument(
        "--table-name", 
        required=True, 
        help="Table name")
    args = parser.parse_args()

    clusterizer = Clusterizer()
    clusterizer.run(args.table_name)
