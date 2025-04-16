# ml-big-data-lab-5

- pull the `jupyter/all-spark-notebook` image

```bash
docker pull jupyter/all-spark-notebook:spark-3.5.0
```

- create `sparkdata` folder

- run docker image and map `sparkdata` directory to the container

```bash
docker run -d -P --name pyspark-notebook -v /Users/datasciencefm/workspace/sparkdata:/sparkdata jupyter/all-spark-notebook:spark-3.5.0
```

- execute the following command to know which host port has been mapped to the container's port 8888

```bash
docker port pyspark-notebook 8888
```

- fetch the notebook token. Your output should resemble this URL: `http://127.0.0.1:8888/lab?token=YOUR_TOKEN_HERE`

```bash
docker logs --tail 3 pyspark-notebook
```

- Replace the default port in the URL with the one you identified

## Usefule materials

- [Setting Up a PySpark Notebook using Docker](https://datascience.fm/setting-up-a-pyspark-notebook-using-docker/)

- [Series on Apache Spark Performance Tuning/Optimisation](https://www.youtube.com/playlist?list=PLWAuYt0wgRcLCtWzUxNg4BjnYlCZNEVth)
