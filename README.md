# ml-big-data-lab-5

## Description

Stack: Cassandra, PySpark

## PySpark configuration

- execute the following command to know which host port has been mapped to the container's port 8888

```bash
docker port pyspark-notebook 8888
```

- fetch the notebook token. Your output should resemble this URL: `http://127.0.0.1:8888/lab?token=YOUR_TOKEN_HERE`

```bash
docker logs --tail 3 pyspark-notebook
```

- Replace the default port in the URL with the one you identified

## Load data to Cassandra

- create table

```bash
docker exec app python src/create_database.py \
    --table-name openfoodfacts \
    --csv-path ./sparkdata/en.openfoodfacts.org.products.csv \
    --delimiter '\t'
```

- execute inside `cassandra` container

```bash
docker exec -it cassandra bash

wget https://downloads.datastax.com/dsbulk/dsbulk-1.11.0.tar.gz

tar -xzf dsbulk-1.11.0.tar.gz -C /opt/

echo 'export PATH=/opt/dsbulk-1.11.0/bin:$PATH' >> ~/.bashrc

source ~/.bashrc

dsbulk --version

# en.openfoodfacts.org.products.csv
dsbulk load \
    -url /sparkdata/en.openfoodfacts.org.products.csv \
    -k mykeyspace \
    -t openfoodfacts \
    --connector.csv.delimiter "\t" \
    --schema.allowMissingFields true \
    --codec.nullStrings "NULL" \
    --connector.csv.maxCharsPerColumn -1 \
    --connector.csv.maxRecords 1894492

# googleplaystore_user_reviews.csv
dsbulk load \
    -url /sparkdata/googleplaystore_user_reviews.csv \
    -k mykeyspace \
    -t user_reviews \
    --schema.allowMissingFields true \
    --codec.nullStrings "NULL" \
    --connector.csv.maxCharsPerColumn -1 \
    -m "0 = indexcolumn, 1 = app, 2 = translated_review, 3 = sentiment, 4 = sentiment_polarity, 5 = sentiment_subjectivity"
```

## Usefule materials

- [Setting Up a PySpark Notebook using Docker](https://datascience.fm/setting-up-a-pyspark-notebook-using-docker/)

- [Series on Apache Spark Performance Tuning/Optimisation](https://www.youtube.com/playlist?list=PLWAuYt0wgRcLCtWzUxNg4BjnYlCZNEVth)
