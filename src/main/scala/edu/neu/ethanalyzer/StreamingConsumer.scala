package edu.neu.ethanalyzer

import edu.neu.ethanalyzer.JdbcConfig.{connectionProperties, jdbcUrl}
import edu.neu.ethanalyzer.Schemas.{blocks_schema, transactions_schema}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StringType


/*
Reads the data stream from Kafka topics through the Spark-Streaming API, and writes it
to MySQL database in realtime using JDBC connection string.
*/
object StreamingConsumer {

  def main(args: Array[String]): Unit = {
    // Initiate Spark session
    val spark: SparkSession = SparkSession
      .builder()
      .appName("EthereumAnalytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Subscribe to kafka topic to listen to transaction data
    val trans_data = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "eth_transactions")
      .load()
      .select(col("value").cast(StringType).as("col"))
      .select(from_json(col("col"), transactions_schema).alias("transaction"))

    trans_data.printSchema()

    // Subscribe to kafka topic to listen to block data
    val blocks_data = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "eth_blocks")
      .load()
      .select(col("value").cast(StringType).as("col"))
      .select(from_json(col("col"), blocks_schema).alias("block"))

    blocks_data.printSchema()

    // Query to store streamed transaction data into mysql database
    val transQuery = trans_data
      .select("transaction.*")
      .writeStream
      .trigger(Trigger.ProcessingTime(2000))
      .foreachBatch({ (batchDF: Dataset[Row], _: Long) => {
        batchDF.write.mode("append")
          .jdbc(jdbcUrl, "transactions_stream", connectionProperties)
      }})
      .start()

    // Query to store streamed block data into mysql database
    val blockQuery = blocks_data
      .select("block.*")
      .writeStream
      .trigger(Trigger.ProcessingTime(2000))
      .foreachBatch({ (batchDF: Dataset[Row], _: Long) => {
        batchDF.write.mode("append")
          .jdbc(jdbcUrl, "blocks_stream", connectionProperties)
      }})
      .start()

    // Terminate connection and stop spark
    transQuery.awaitTermination()
    blockQuery.awaitTermination()
    spark.stop()
  }
}