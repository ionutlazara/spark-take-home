# Data transformations with Python

This coding challenge is a collection of Python jobs that are supposed to extract, transform and load data. These jobs are using PySpark to process larger volumes of data and are supposed to run on a Spark cluster (via spark-submit).

## Local Setup

**Pre-requisites**

- Python 3.8+
- Java (11)
- Docker


**Install dependencies**

```bash
pip install -r requirements.txt
```

**Docker setup**

Make sure your docker engine is up and running:

```bash
docker --version
```

If you get an error above, execute:

PowerShell:
```powershell
set-alias docker-start "C:\Program Files\Docker\Docker\Docker Desktop.exe"
docker-start
```

Bash:
```bash
dockerd
```

In your project root, add a file called ```.env.spark```, containing:
```
SPARK_NO_DAEMONIZE=true
```

Run:
```
docker-compose up -d --scale spark-worker=3
```

You should have now 5 containers up and running, as follows:
```
[+] Running 5/5
 ✔ Container da-spark-master                 Running                                                                                                                                                                           0.0s 
 ✔ Container spark-take-home-spark-worker-1  Running                                                                                                                                                                           0.0s 
 ✔ Container spark-take-home-spark-worker-2  Running                                                                                                                                                                           0.0s 
 ✔ Container spark-take-home-spark-worker-3  Running                                                                                                                                                                           0.0s 
 ✔ Container da-spark-history                Running  
```

You can access the spark UI via *http://localhost:9090/* and the Spark History Server via *http://localhost:18080/*


## Running Jobs

Before executing the spark jobs, we need to generate the csv datasets, as follows:
```
python utils/generate_csv_dataset.py
```
Two csv files will be created in the following location: ```dataset/```

In our pipeline we have 4 jobs (not including the _optimized versions), as follows:

**CalculateMetricsCsv**

We use *spark-submit* to run the jobs in our containerized spark cluster
```
docker exec -it da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client --name CalculateMetricsCsv ./spark_jobs/calculate_dau_mau.py
```

Result:
```
+----------+------------------+
|      date|daily_active_users|
+----------+------------------+
|2023-12-31|              2533|
|2024-01-01|              2498|
|2024-01-02|              2545|
|2024-01-03|              2392|
|2024-01-04|              2552|
|2024-01-05|              2589|
|2024-01-06|              2622|
|2024-01-07|              2494|
|2024-01-08|              2449|
|2024-01-09|              2572|
|2024-01-10|              2545|
|2024-01-11|              2645|
|2024-01-12|              2504|
|2024-01-13|              2677|
|2024-01-14|              2557|
|2024-01-15|              2597|
|2024-01-16|              2575|
|2024-01-17|              2533|
|2024-01-18|              2622|
|2024-01-19|              2628|
+----------+------------------+
only showing top 20 rows

```
```
+----+-----+--------------------+
|year|month|monthly_active_users|
+----+-----+--------------------+
|2023|   12|                2533|
|2024|    1|               76651|
|2024|    2|               72245|
|2024|    3|               76950|
|2024|    4|               74330|
|2024|    5|               77507|
|2024|    6|               75099|
|2024|    7|               76894|
|2024|    8|               76914|
|2024|    9|               74766|
|2024|   10|               76415|
|2024|   11|               74541|
|2024|   12|               52912|
+----+-----+--------------------+
```

**CalculateSessionMetrics**

We use *spark-submit* to run the jobs in our containerized spark cluster
```
docker exec -it da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client --name CalculateSessionMetrics ./spark_jobs/calculate_session_metrics.py
```
Result:
```
+-------+------------------------+-----------------------+
|user_id|avg_session_duration_sec|avg_actions_per_session|
+-------+------------------------+-----------------------+
|u000127|                  229.19|                    1.0|
|u000139|                197.8535|                    1.0|
|u000216|      153.07250000000002|                    1.0|
|u000270|                  88.181|                    1.0|
|u000309|                 178.008|                    1.0|
|u000376|                 89.5015|                    1.0|
|u000385|                 103.062|                    1.0|
|u000392|                 267.063|                    1.0|
|u000457|                 199.336|                    1.0|
|u000480|                 176.826|                    1.0|
|u000496|      183.28599999999997|                    1.0|
|u000498|                  91.899|                    1.0|
|u000510|                 223.036|                    1.0|
|u000532|                  64.522|                    1.0|
|u000581|                 110.943|                    1.0|
|u000614|                   5.768|                    1.0|
|u000657|                 295.669|                    1.0|
|u000699|                108.5475|                    1.0|
|u000754|                 273.402|                    1.0|
|u000770|                  88.017|                    1.0|
+-------+------------------------+-----------------------+
only showing top 20 rows
```

**ParquetDataset**

We use *spark-submit* to run the jobs in our containerized spark cluster
```
docker exec -it da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client --name ParquetDataset ./spark_jobs/dataset_parquet.py
```

Result:
Two parquet datasets in ```dataset/output```.

**JoinStrategy**

We use *spark-submit* to run the jobs in our containerized spark cluster
```
docker exec -it da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client --name JoinStrategy ./spark_jobs/dataset_join.py
```

Result:
```
+-------+-------------------+-----------+-------+-----------+-----------+-------+----------+-------+--------------+-----------------+
|user_id|          timestamp|action_type|page_id|duration_ms|app_version|user_id| join_date|country|   device_type|subscription_type|
+-------+-------------------+-----------+-------+-----------+-----------+-------+----------+-------+--------------+-----------------+
|u223628|2024-03-09 19:59:34|  page_view|p409951|     210313|      5.5.6|u223628|2022-12-09|     AU|       Windows|            basic|
|u810759|2024-02-27 22:49:06|       edit|p343104|      40146|      7.3.4|u810759|2022-03-10|     JP|           Mac|             free|
|u291623|2024-08-11 12:03:38|     create|p604903|     121498|      7.9.8|u291623|2021-11-26|     AU|        iPhone|            basic|
|u377931|2024-12-13 08:06:08|  page_view|p454034|      22891|      5.2.3|u377931|2020-10-11|     CA|        iPhone|             free|
|u957103|2024-12-14 07:59:45|  page_view|p250870|     280053|      7.0.8|u957103|2020-12-04|     UK|       Windows|          premium|
|u876863|2024-07-16 22:11:32|       edit|p974154|     177497|      5.6.3|u876863|2021-06-11|     UK|       Windows|             free|
|u905501|2024-04-14 02:43:53|     create|p599999|     180042|      7.8.5|u905501|2024-12-18|     AU|Android Tablet|            basic|
|u476396|2024-09-02 07:23:02|     create|p930622|     274790|      5.7.2|u476396|2021-02-04|     MX|           Mac|       enterprise|
|u476396|2024-09-02 07:23:02|     create|p930622|     274790|      5.7.2|u476396|2021-10-23|     US|Android Tablet|            basic|
|u592771|2024-09-05 21:49:53|  page_view|p492603|     212805|      7.8.4|u592771|2023-01-17|     MX|        iPhone|          premium|
|u920194|2024-02-08 22:22:15|  page_view|p200718|     247639|      7.7.5|u920194|2022-06-02|     DE|          iPad|       enterprise|
|u554877|2024-03-13 11:24:29|     delete|p103139|     172711|      6.5.1|u554877|2022-05-26|     DE|Android Tablet|            basic|
|u017595|2024-09-09 07:27:10|      share|p126363|     291528|      7.2.0|u017595|2024-01-25|     IN|          iPad|            basic|
|u589680|2024-06-21 19:09:18|     delete|p894205|      79347|      5.0.5|u589680|2022-11-06|     MX|        iPhone|            basic|
|u541937|2024-09-13 08:52:26|      share|p253349|     267465|      5.7.6|u541937|2021-01-13|     MX|           Mac|             free|
|u980790|2024-03-20 06:07:42|  page_view|p351281|      88382|      7.8.1|u980790|2020-02-15|     AU|          iPad|            basic|
|u426211|2024-07-05 12:18:47|     create|p243743|      47890|      6.1.5|u426211|2020-09-14|     UK| Android Phone|             free|
|u844505|2024-08-29 20:02:22|     create|p495944|      20640|      6.3.8|u844505|2021-06-28|     AU|        iPhone|             free|
|u538023|2024-02-18 18:06:12|     create|p578284|      79204|      6.9.4|u538023|2024-05-29|     MX|        iPhone|            basic|
|u077940|2024-10-08 01:29:00|     delete|p614986|      17412|      7.7.7|u077940|2020-09-03|     IN|       Windows|            basic|
+-------+-------------------+-----------+-------+-----------+-----------+-------+----------+-------+--------------+-----------------+
only showing top 20 rows
```

## Running Tests

**Run unit tests**
```
pytest tests\unit\ 
```
