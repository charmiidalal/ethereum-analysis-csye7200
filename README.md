# Ethereum-Analytics-Dashboard

A dashboard to analyze transactions on the Ethereum blockchain in real-time. We used the publicly available [crypto_ethereum](https://cloud.google.com/blog/products/data-analytics/ethereum-bigquery-public-dataset-smart-contract-analytics) dataset from Bigquery.
The data was downloaded and stored locally as csv files.

## Requirements

1. `scala 2.12.15`
2. `sbt 1.5.8`
3. `Apache Spark 3.2.1`
4. `docker` and `docker-compose`


## Instructions to Run

1. Clone and `cd` into the repo

2. Use `docker-compose up -d` to start all the containers in a detached mode. The configs are defined in the `docker-compose.yml` file.
   The services will be exposed to the following ports:

    - `Zookeeper` (required for kafka): 2181
    - `Kafka`: 9092
    - `Superset`: 8088
    - `Mysql`: 3306

3. Setup Apache-Superset through the following command (this will configure superset and you will be able to connect to it on port **8088**:

    - ```docker exec -it superset superset-init```


4. Use this sequence of commands to start the Producer adn Consumer scripts:

    - `sbt assembly` -> This will create the .jar file for the project using the **assembly** plugin

    - Run the consumer using:
    - ```spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 --master localhost --class "edu.neu.ethanalyzer.TransactionsConsumer" ./target/scala-2.12/EthereumAnalytics-assembly-1.0.jar```
    - Producer using:
    - ```spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 --master localhost --class "edu.neu.ethanalyzer.RowWiseProducer" ./target/scala-2.12/EthereumAnalytics-assembly-1.0.jar```
